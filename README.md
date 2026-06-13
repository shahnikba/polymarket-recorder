# polymarket-recorder

Continuous, research-only recorder for **public** Polymarket market data:
order-book updates, price changes, and trades streamed from the public market
WebSocket channel, with periodic universe selection and archival to S3.

Read-only. Uses the Gamma API, the CLOB public data endpoints, and the public
market WebSocket. No wallet, no auth, no trading.

## Architecture

One long-lived process. **Capture is continuous; only the flusher and the
universe refresh run on timers** — there is no cron-style "poll every few
minutes", which would lose all the intra-interval book dynamics.

```
                 Gamma /markets (rank by volume24hr)
                          |
                 UniverseManager  --refine via--> CLOB /book (spread+depth)
                          |  target token_ids
                          v
   +------ orchestrator (main.py) ------ diff -> add/remove on shards
   |                 |                |
   v                 v                v
CaptureShard 0   CaptureShard 1   CaptureShard N      (continuous WS)
   \                 |                /
    \   record() every frame (non-blocking)
     v               v               v
            RawArchiver  (asyncio.Queue -> JSONL on disk, rotate)
                          |  rotated files
                          v
                 pending_dir  --every flush_interval--> S3Flusher --> S3 (gzip)
```

Each shard owns one WS connection for a slice of the asset universe. On every
(re)connect it subscribes, then takes a REST `/book` snapshot per asset
(rate-limited, archived) before trusting incremental updates — the same
snapshot+increment discipline you'd use on a FIX feed. Disconnects are written
to the archive as explicit `lifecycle` records so gaps are visible, not silent.

The **raw archive is the source of truth**: every frame is stored verbatim with
a local receipt timestamp, append-only. Normalisation/parsing happens downstream
off that immutable log, so a parser bug is always replayable. Suggested
downstream: read the gzipped JSONL from S3 into Parquet partitioned by
date/market, query with DuckDB.

## Layout

```
src/pmrec/
  config.py      dataclass config from YAML (secrets via IAM/env, not here)
  ratelimit.py   shared async rate limiter (keeps REST under ~60/min)
  gamma.py       Gamma client: rank active markets by 24h volume
  clob.py        CLOB client: public order-book snapshots
  universe.py    two-stage selection (volume shortlist -> book-depth refine) + diff
  capture.py     WS shard: connect/subscribe/snapshot/ping/recv/reconnect
  archiver.py    non-blocking frame queue -> rotating JSONL files
  flusher.py     gzip + upload rotated files to S3
  main.py        orchestrator + entrypoint + graceful shutdown
  backfill.py    SEPARATE historical batch job (Data API trades) -- scaffold
deploy/pmrec.service   systemd unit for the server
config.example.yaml    copy to config.yaml and edit
```

## Run locally

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
pip install -e .          # or: export PYTHONPATH=src
cp config.example.yaml config.yaml
# leave s3.bucket = CHANGE_ME to keep files local while developing
PYTHONPATH=src python -m pmrec.main
```

Frames accumulate under `./data/pending` until you set a real bucket.

## Run on the server

```bash
sudo mkdir -p /opt/polymarket-recorder && cd /opt/polymarket-recorder
# copy the repo here, create .venv, pip install -r requirements.txt
# put config.yaml here with your real s3.bucket
sudo cp deploy/pmrec.service /etc/systemd/system/
sudo systemctl daemon-reload && sudo systemctl enable --now pmrec
journalctl -u pmrec -f
```

Give the instance an IAM role with `s3:PutObject` on your bucket; boto3 picks it
up automatically (no keys in config). Put the instance in **us-east-1** for the
most stable connection to Polymarket's infra. Add an S3 lifecycle rule to move
the prefix to Glacier after ~90 days.

## Historical backfill

The live recorder only captures from the moment it starts. To populate earlier
history (or patch gaps your lifecycle `disconnect` records flag), run the
separate batch job against the public Data API `/trades` endpoint:

```bash
PYTHONPATH=src python -m pmrec.backfill 0xCONDITION_ID [0xCONDITION_ID ...]
# include maker-side trades too (default is taker-only):
PYTHONPATH=src python -m pmrec.backfill --include-maker 0xCONDITION_ID
```

It pages by `offset` (capped at 10000 by the API — markets with deeper history
than that need on-chain indexing for the tail; the job logs when it hits the
cap), wraps each trade in the **same frame envelope** the live recorder uses
(`source: "backfill_trade"`), and hands the files to the **same S3 flusher** —
so backfill and live capture land in one store with one schema. With
`s3.bucket = CHANGE_ME` the files stay local in `data/pending` for inspection.

## Tests

```bash
pip install pytest
PYTHONPATH=src python -m pytest tests/ -q
```

Fully offline: HTTP is stubbed with `httpx.MockTransport` and S3 with a fake
boto3 client — no network, no AWS, no live Polymarket calls.

## Things left for you (tuning, not gaps)

- **Ping wire format** (`capture.py`): sends literal `"PING"`. If the server
  drops connections, try `"{}"`. Confirm against the live feed.
- **Per-connection subscription cap** (`config.py: max_assets_per_connection`):
  a cap has been reported but the exact value on the main CLOB channel is
  unverified — keep it modest and shard.
- **Refinement scoring** (`universe.py: _refine`): ships with a sensible default
  (`sqrt(volume) * depth**0.25 / spread`); the three exponents are knobs to
  re-weight for your research definition of liquidity.
- **Normalisation**: a downstream job from raw JSONL -> Parquet (book
  reconstruction from snapshot + increments) is intentionally not included.

## Note

This records public data only and never trades. The market WebSocket and the
`/book` and `/markets` endpoints require no auth, no wallet, no account.
