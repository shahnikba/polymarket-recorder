"""A tiny async rate limiter: guarantees a minimum interval between calls.

Shared across all capture shards so that reconnect storms (each shard
re-fetching REST book snapshots on reconnect) cannot collectively blow past
the Gamma/CLOB ~60 req/min budget.
"""
from __future__ import annotations

import asyncio
import time


class AsyncRateLimiter:
    def __init__(self, min_interval_s: float) -> None:
        self._min_interval = min_interval_s
        self._lock = asyncio.Lock()
        self._last = 0.0

    async def acquire(self) -> None:
        async with self._lock:
            now = time.monotonic()
            wait = self._min_interval - (now - self._last)
            if wait > 0:
                await asyncio.sleep(wait)
            self._last = time.monotonic()
