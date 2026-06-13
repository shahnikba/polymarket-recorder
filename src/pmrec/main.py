"""Orchestrator / entrypoint.

Wires everything together and runs the continuous recorder:

  - one RawArchiver writer task (drains the frame queue to disk)
  - one S3Flusher task (periodic upload of rotated files)
  - N CaptureShard tasks (continuous WS capture)
  - one universe-refresh loop (re-select every refresh_interval_s, diff, and
    apply add/remove deltas across shards)

Capture is continuous; only the flusher and the universe refresh run on timers.
Graceful shutdown on SIGINT/SIGTERM flushes everything before exit.
"""
from __future__ import annotations

import asyncio
import signal

import httpx

from .archiver import RawArchiver
from .capture import CaptureShard
from .clob import ClobClient
from .config import Config
from .flusher import S3Flusher
from .gamma import GammaClient
from .ratelimit import AsyncRateLimiter
from .universe import UniverseManager, diff_universe


def _shard_for(token_id: str, n_shards: int) -> int:
    return hash(token_id) % n_shards


class Recorder:
    def __init__(self, cfg: Config) -> None:
        self.cfg = cfg
        self._http = httpx.AsyncClient(timeout=15.0)
        self._limiter = AsyncRateLimiter(cfg.rate_limit.rest_min_interval_s)
        self.gamma = GammaClient(cfg.endpoints.gamma_base, self._limiter, self._http)
        self.clob = ClobClient(cfg.endpoints.clob_base, self._limiter, self._http)
        self.universe = UniverseManager(cfg.universe, self.gamma, self.clob)
        self.archiver = RawArchiver(
            cfg.archive.spool_dir, cfg.archive.pending_dir,
            cfg.archive.rotate_interval_s, cfg.archive.rotate_max_bytes,
        )
        self.flusher = S3Flusher(
            cfg.archive.pending_dir, cfg.s3.bucket, cfg.s3.prefix,
            cfg.s3.flush_interval_s, cfg.s3.region, cfg.s3.delete_after_upload,
        )
        self.shards: list[CaptureShard] = []
        self._current: set[str] = set()
        self._stop = asyncio.Event()

    async def run(self) -> None:
        # Initial selection so shards start with assets.
        target, _ = await self.universe.select()
        n_shards = max(1, -(-len(target) // self.cfg.capture.max_assets_per_connection))
        print(f"initial universe: {len(target)} assets across {n_shards} shards",
              flush=True)

        self.shards = [
            CaptureShard(i, self.cfg.capture, self.cfg.endpoints.ws_market,
                         self.clob, self.archiver, self._limiter)
            for i in range(n_shards)
        ]
        for tid in target:
            self.shards[_shard_for(tid, n_shards)].assets.add(tid)
        self._current = set(target)

        tasks = [
            asyncio.create_task(self.archiver.run(), name="archiver"),
            asyncio.create_task(self.flusher.run(), name="flusher"),
            asyncio.create_task(self._universe_loop(), name="universe"),
        ]
        for s in self.shards:
            tasks.append(asyncio.create_task(s.run(), name=f"shard-{s.shard_id}"))

        await self._stop.wait()
        await self._shutdown(tasks)

    async def _universe_loop(self) -> None:
        while not self._stop.is_set():
            try:
                await asyncio.wait_for(self._stop.wait(),
                                       timeout=self.cfg.universe.refresh_interval_s)
                return
            except asyncio.TimeoutError:
                pass
            try:
                target, _ = await self.universe.select()
            except Exception as e:                          # noqa: BLE001
                print(f"WARN: universe refresh failed: {e}", flush=True)
                continue
            to_add, to_remove = diff_universe(self._current, target)
            if not to_add and not to_remove:
                continue
            print(f"universe delta: +{len(to_add)} -{len(to_remove)}", flush=True)
            n = len(self.shards)
            by_shard_add: dict[int, set[str]] = {}
            by_shard_rm: dict[int, set[str]] = {}
            for tid in to_add:
                by_shard_add.setdefault(_shard_for(tid, n), set()).add(tid)
            for tid in to_remove:
                by_shard_rm.setdefault(_shard_for(tid, n), set()).add(tid)
            for sid, assets in by_shard_add.items():
                await self.shards[sid].add(assets)
            for sid, assets in by_shard_rm.items():
                await self.shards[sid].remove(assets)
            self._current = target

    def stop(self) -> None:
        self._stop.set()

    async def _shutdown(self, tasks: list[asyncio.Task]) -> None:
        print("shutting down...", flush=True)
        for s in self.shards:
            s.stop()
        self.archiver.stop()
        self.flusher.stop()
        await asyncio.sleep(0.5)
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        await self._http.aclose()


def main() -> None:
    cfg = Config.load()
    rec = Recorder(cfg)
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, rec.stop)
    loop.run_until_complete(rec.run())


if __name__ == "__main__":
    main()
