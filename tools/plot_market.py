#!/usr/bin/env python3
"""Plot recorded Polymarket odds for a market over a time window.

Reconstructs the implied-probability (best-bid/ask midpoint) time series for one
or more markets straight from the gzipped JSONL frames the recorder wrote to S3,
and renders a PNG + CSV. This is the read side of the raw archive: it never hits
the live market, only your own recorded data.

Resolution order for *which tokens* to chart:
  --token  ID[:LABEL]   use these token ids directly (no Gamma needed)
  --condition-id 0x..   resolve a market's Yes token via Gamma
  --market "text"       match Gamma question text (may match several markets;
                        each matched market contributes its Yes line — this is
                        how you chart e.g. all four "peace deal by <date>" markets)

Examples:
  # all "US x Iran permanent peace deal" deadlines, around the announcement
  python tools/plot_market.py --market "Iran permanent peace deal" \
      --start 2026-06-14T20:00 --end 2026-06-14T23:00

  # one market by condition id, last 3 hours (default window)
  python tools/plot_market.py --condition-id 0xabc...

  # offline / Gamma down: chart specific tokens you already know
  python tools/plot_market.py --token 590469...:"by June 15" --start ... --end ...

S3 location and AWS creds: bucket/prefix default to your config.yaml (S3Config);
override with --bucket/--prefix. Credentials come from the usual AWS chain
(AWS_PROFILE / env / instance role), same as the recorder.
"""
from __future__ import annotations

import argparse
import gzip
import io
import json
import os
import sys
from datetime import datetime, timedelta, timezone

import boto3
import httpx

# Import the recorder's own config so bucket/prefix default to your setup.
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))
from pmrec.config import Config  # noqa: E402

GAMMA = "https://gamma-api.polymarket.com"
ROTATE_SLACK_S = 600  # a file may hold frames up to ~rotate_interval after its name ts


# -- time helpers ----------------------------------------------------------
def parse_ts(s: str) -> datetime:
    for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M", "%Y-%m-%d %H:%M:%S",
                "%Y-%m-%d %H:%M", "%Y-%m-%d"):
        try:
            return datetime.strptime(s, fmt).replace(tzinfo=timezone.utc)
        except ValueError:
            continue
    raise SystemExit(f"unparseable time: {s!r} (use e.g. 2026-06-14T21:00)")


def file_start(key: str) -> datetime | None:
    base = os.path.basename(key)               # frames_20260614T201019_...jsonl.gz
    try:
        return datetime.strptime(base.split("_")[1], "%Y%m%dT%H%M%S").replace(
            tzinfo=timezone.utc)
    except (IndexError, ValueError):
        return None


# -- market resolution -----------------------------------------------------
def gamma_markets() -> list[dict]:
    out, seen = [], set()
    with httpx.Client(timeout=30) as h:
        for closed in ("false", "true"):
            r = h.get(f"{GAMMA}/markets", params={
                "closed": closed, "limit": "1000",
                "order": "volume24hr", "ascending": "false"})
            r.raise_for_status()
            for m in r.json():
                cid = m.get("conditionId") or m.get("condition_id")
                if cid and cid not in seen:
                    seen.add(cid)
                    out.append(m)
    return out


def _yes_token(m: dict) -> str | None:
    toks = json.loads(m["clobTokenIds"])
    outs = json.loads(m["outcomes"])
    for i, o in enumerate(outs):
        if str(o).lower() == "yes":
            return str(toks[i])
    return str(toks[0]) if toks else None


def resolve_tokens(args) -> dict[str, str]:
    """Return {token_id: label} to chart."""
    if args.token:
        out = {}
        for spec in args.token:
            tid, _, label = spec.partition(":")
            out[tid.strip()] = label.strip() or tid[:10]
        return out
    markets = gamma_markets()
    picked = []
    if args.condition_id:
        picked = [m for m in markets
                  if (m.get("conditionId") or m.get("condition_id")) == args.condition_id]
    elif args.market:
        q = args.market.lower()
        picked = [m for m in markets if q in m.get("question", "").lower()]
    if not picked:
        raise SystemExit("no market matched. Try --market text / --condition-id / --token.")
    out = {}
    for m in picked:
        tid = _yes_token(m)
        if tid:
            out[tid] = shorten_label(m.get("question", ""), len(picked) > 1)
    print(f"resolved {len(out)} market(s):", flush=True)
    for tid, lbl in out.items():
        print(f"  {lbl}  ({tid[:12]}…)", flush=True)
    return out


def shorten_label(q: str, multi: bool) -> str:
    if not multi:
        return q[:60]
    # When several markets share a stem, the tail (a date/outcome) is the useful bit.
    for sep in (" by ", ": ", " - "):
        if sep in q:
            return q.split(sep, 1)[1].rstrip("?")
    return q[:40]


# -- S3 read ---------------------------------------------------------------
def list_keys(s3, bucket: str, prefix: str, start: datetime, end: datetime) -> list[str]:
    keys = []
    day = start.date()
    while day <= end.date():
        p = f"{prefix.strip('/')}/{day:%Y/%m/%d}/"
        token = None
        while True:
            kw = dict(Bucket=bucket, Prefix=p)
            if token:
                kw["ContinuationToken"] = token
            resp = s3.list_objects_v2(**kw)
            for o in resp.get("Contents", []):
                fs = file_start(o["Key"])
                if fs is None:
                    continue
                # keep files whose window overlaps [start, end]
                if fs <= end and fs + timedelta(seconds=ROTATE_SLACK_S) >= start:
                    keys.append(o["Key"])
            if resp.get("IsTruncated"):
                token = resp["NextContinuationToken"]
            else:
                break
        day += timedelta(days=1)
    return sorted(keys)


def mid_from_item(it: dict) -> float | None:
    if "best_bid" in it and "best_ask" in it:
        try:
            return (float(it["best_bid"]) + float(it["best_ask"])) / 2
        except (TypeError, ValueError):
            return None
    if "bids" in it or "asks" in it:
        bb = max((float(x["price"]) for x in (it.get("bids") or [])), default=None)
        aa = min((float(x["price"]) for x in (it.get("asks") or [])), default=None)
        if bb is not None and aa is not None:
            return (bb + aa) / 2
    return None


def extract_series(s3, bucket, keys, watch, start, end):
    """watch: {token_id: label} -> events list [(ts, label, pct)] sorted."""
    ids = tuple(watch)
    w0, w1 = start.timestamp(), end.timestamp()
    events = []
    for i, key in enumerate(keys, 1):
        body = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
        with gzip.open(io.BytesIO(body), "rt") as fh:
            for line in fh:
                if not any(t in line for t in ids):     # fast skip
                    continue
                r = json.loads(line)
                ts = r["recv_ts"]
                if ts < w0 or ts > w1:
                    continue
                p, s = r["payload"], r["source"]
                if s == "ws" and isinstance(p, dict) and "price_changes" in p:
                    its = p["price_changes"]
                elif s == "ws" and isinstance(p, list):
                    its = p
                elif s == "rest_snapshot":
                    its = [p]
                else:
                    continue
                for it in its:
                    if not isinstance(it, dict):
                        continue
                    lbl = watch.get(it.get("asset_id"))
                    if not lbl:
                        continue
                    m = mid_from_item(it)
                    if m is not None:
                        events.append((ts, lbl, m * 100))
        print(f"  [{i}/{len(keys)}] {os.path.basename(key)}", flush=True)
    events.sort()
    return events


# -- output ----------------------------------------------------------------
def write_csv(events, watch, path):
    labels = list(dict.fromkeys(watch.values()))
    cur = {l: "" for l in labels}
    with open(path, "w", newline="") as f:
        f.write("time_utc,epoch," + ",".join(labels) + "\n")
        for ts, lbl, pct in events:
            cur[lbl] = f"{pct:.2f}"
            t = datetime.fromtimestamp(ts, timezone.utc).strftime("%H:%M:%S.%f")[:-3]
            f.write(f"{t},{ts:.3f}," + ",".join(cur[l] for l in labels) + "\n")


def make_plot(events, watch, title, path, annotate):
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
    except ImportError:
        print("matplotlib not installed; wrote CSV only. `pip install matplotlib` for PNG.",
              flush=True)
        return False
    labels = list(dict.fromkeys(watch.values()))
    series = {l: ([], []) for l in labels}
    cur = {}
    for ts, lbl, pct in events:
        cur[lbl] = pct
        dt = datetime.fromtimestamp(ts, timezone.utc)
        for l in labels:
            if l in cur:
                series[l][0].append(dt)
                series[l][1].append(cur[l])
    plt.figure(figsize=(14, 6.5))
    for l in labels:
        xs, ys = series[l]
        if xs:
            plt.plot(xs, ys, lw=1.3, label=l)
    if annotate:
        at = parse_ts(annotate[0])
        plt.axvline(at, ls="--", color="black", lw=1)
        plt.annotate(annotate[1], (at, 8), xytext=(8, 0),
                     textcoords="offset points", fontsize=9)
    plt.ylabel("implied probability (%)")
    plt.xlabel("time (UTC)")
    plt.title(title)
    plt.legend(loc="best")
    plt.grid(alpha=0.3)
    plt.ylim(-3, 103)
    plt.tight_layout()
    plt.savefig(path, dpi=130)
    return True


def main() -> None:
    ap = argparse.ArgumentParser(description="Plot recorded Polymarket odds from S3.")
    g = ap.add_mutually_exclusive_group(required=True)
    g.add_argument("--market", help="match Gamma question text (substring)")
    g.add_argument("--condition-id", help="exact market condition id (0x…)")
    g.add_argument("--token", action="append",
                   help="token id, optionally TOKEN:LABEL (repeatable; skips Gamma)")
    ap.add_argument("--start", help="UTC start, e.g. 2026-06-14T20:00 (default: end-3h)")
    ap.add_argument("--end", help="UTC end (default: now)")
    ap.add_argument("--bucket", help="S3 bucket (default: config.yaml s3.bucket)")
    ap.add_argument("--prefix", help="S3 prefix (default: config.yaml s3.prefix)")
    ap.add_argument("--region", help="AWS region (default: config.yaml s3.region)")
    ap.add_argument("--out", default="market_odds",
                    help="output basename (writes .png and .csv)")
    ap.add_argument("--title", help="plot title")
    ap.add_argument("--annotate", nargs=2, metavar=("UTC_TIME", "TEXT"),
                    help="draw a labelled vertical line, e.g. 2026-06-14T21:16 'news'")
    args = ap.parse_args()

    cfg = Config.load()
    bucket = args.bucket or cfg.s3.bucket
    prefix = args.prefix or cfg.s3.prefix
    region = args.region or cfg.s3.region
    if bucket in (None, "", "CHANGE_ME"):
        raise SystemExit("no S3 bucket: set s3.bucket in config.yaml or pass --bucket")

    end = parse_ts(args.end) if args.end else datetime.now(timezone.utc)
    start = parse_ts(args.start) if args.start else end - timedelta(hours=3)

    watch = resolve_tokens(args)
    s3 = boto3.client("s3", region_name=region)
    print(f"listing s3://{bucket}/{prefix} for {start:%Y-%m-%d %H:%M}–{end:%H:%M} UTC…",
          flush=True)
    keys = list_keys(s3, bucket, prefix, start, end)
    if not keys:
        raise SystemExit("no recorded files in that window.")
    print(f"reading {len(keys)} file(s)…", flush=True)
    events = extract_series(s3, bucket, keys, watch, start, end)
    if not events:
        raise SystemExit("no ticks for those tokens in the window.")

    csv_path, png_path = args.out + ".csv", args.out + ".png"
    write_csv(events, watch, csv_path)
    title = args.title or (args.market or args.condition_id or "Polymarket") + " — recorded odds"
    made = make_plot(events, watch, title, png_path, args.annotate)
    print(f"\n{len(events)} ticks across {len(set(l for _,l,_ in events))} line(s)")
    print(f"wrote {csv_path}" + (f" and {png_path}" if made else ""), flush=True)


if __name__ == "__main__":
    main()
