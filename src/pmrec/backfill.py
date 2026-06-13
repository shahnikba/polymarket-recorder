"""Historical backfill (separate one-shot / scheduled batch job).

This is NOT the live recorder. Use it to populate the period *before* you
started recording, and to patch gaps your lifecycle 'disconnect' records flag.

Polymarket history sources (public):
  - Gamma: current market metadata + indexed volume/liquidity (no deep tick
    history).
  - Data API (https://data-api.polymarket.com): public trade history / fills,
    holders, positions. This is the main source of historical executed trades.
  - On-chain (Polygon): the Conditional Token Framework + UMA resolution. The
    fully authoritative but heaviest route; index via a Polygon RPC if you need
    settlement-level ground truth.

Output matches the live recorder so both feed one store: each trade is wrapped
in the same frame envelope (recv_ts / recv_iso / source / payload), written
newline-delimited JSON into the archive `pending_dir`, then handed to the same
`S3Flusher` — which gzips and uploads under the date-partitioned prefix, or
(when `s3.bucket` is still ``CHANGE_ME``) just leaves the files local for dev.

Pagination note: the Data API pages by `offset` and caps it at 10000, so a
single market query returns at most 10000 + limit of its most-recent trades.
For markets with deeper history than that, the offset window can't reach the
tail; index on-chain if you need full settlement history. We log when a market
hits the cap so the truncation is explicit, never silent.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os

import httpx

from .archiver import _now_iso
from .config import Config
from .flusher import S3Flusher
from .ratelimit import AsyncRateLimiter

DATA_API_BASE = "https://data-api.polymarket.com"

# The Data API rejects offset values above this.
_MAX_OFFSET = 10_000
# Server-side max page size for /trades.
_MAX_LIMIT = 500


async def fetch_trades(
    http: httpx.AsyncClient,
    limiter: AsyncRateLimiter,
    market: str,
    *,
    limit: int = _MAX_LIMIT,
    taker_only: bool = True,
) -> list[dict]:
    """Fetch historical trades for a single market (condition id), newest first.

    Pages through `/trades` by `offset` until the server returns a short page
    (the end) or the offset cap is reached. Each REST call goes through the
    shared rate limiter so a multi-market backfill stays under budget.
    """
    limit = max(1, min(limit, _MAX_LIMIT))
    trades: list[dict] = []
    offset = 0
    while offset <= _MAX_OFFSET:
        await limiter.acquire()
        resp = await http.get(
            f"{DATA_API_BASE}/trades",
            params={
                "market": market,
                "limit": limit,
                "offset": offset,
                "takerOnly": str(taker_only).lower(),
            },
        )
        resp.raise_for_status()
        page = resp.json()
        if not isinstance(page, list) or not page:
            break
        trades.extend(page)
        if len(page) < limit:
            break                       # last page
        offset += limit
        if offset > _MAX_OFFSET:
            print(
                f"WARN: {market}: hit the Data API offset cap ({_MAX_OFFSET}); "
                f"trades older than the {len(trades)} fetched are not reachable "
                "via offset pagination — index on-chain for the full tail.",
                flush=True,
            )
    return trades


def _write_frames(pending_dir: str, market: str, trades: list[dict]) -> str | None:
    """Write trades as live-recorder frames into pending_dir. Returns the path
    (or None if there was nothing to write)."""
    if not trades:
        return None
    os.makedirs(pending_dir, exist_ok=True)
    safe = market.replace("/", "_")
    path = os.path.join(pending_dir, f"backfill_{safe}_{_now_iso()}.jsonl")
    with open(path, "w") as f:
        for t in trades:
            rec = {
                "recv_ts": t.get("timestamp"),   # trade time (epoch s), if present
                "recv_iso": _now_iso(),          # when we fetched it
                "source": "backfill_trade",
                "shard": -1,                      # -1 = not from a capture shard
                "market": market,
                "payload": t,
            }
            f.write(json.dumps(rec, separators=(",", ":")) + "\n")
    return path


async def run(markets: list[str], *, taker_only: bool = True) -> None:
    cfg = Config.load()
    limiter = AsyncRateLimiter(cfg.rate_limit.rest_min_interval_s)
    written: list[str] = []
    async with httpx.AsyncClient(timeout=30.0) as http:
        for m in markets:
            try:
                trades = await fetch_trades(http, limiter, m, taker_only=taker_only)
            except httpx.HTTPError as e:
                print(f"WARN: {m}: fetch failed: {e}", flush=True)
                continue
            path = _write_frames(cfg.archive.pending_dir, m, trades)
            print(f"{m}: {len(trades)} trades"
                  + (f" -> {os.path.basename(path)}" if path else " (nothing to write)"),
                  flush=True)
            if path:
                written.append(path)

    if not written:
        return
    # Reuse the live recorder's flusher: gzip + upload under the same key
    # scheme, or leave files local when the bucket is still CHANGE_ME.
    flusher = S3Flusher(
        cfg.archive.pending_dir, cfg.s3.bucket, cfg.s3.prefix,
        cfg.s3.flush_interval_s, cfg.s3.region, cfg.s3.delete_after_upload,
    )
    await asyncio.to_thread(flusher._flush_once)
    if cfg.s3.bucket == "CHANGE_ME":
        print(f"bucket is CHANGE_ME; {len(written)} file(s) left in "
              f"{cfg.archive.pending_dir} for local inspection.", flush=True)


def main() -> None:
    ap = argparse.ArgumentParser(description="Polymarket historical backfill")
    ap.add_argument("markets", nargs="+",
                    help="market condition ids (0x… hex) to backfill")
    ap.add_argument("--include-maker", action="store_true",
                    help="include maker-side trades too (default: taker only)")
    args = ap.parse_args()
    asyncio.run(run(args.markets, taker_only=not args.include_maker))


if __name__ == "__main__":
    main()
