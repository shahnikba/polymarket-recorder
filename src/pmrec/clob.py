"""CLOB public REST client.

Only the public, no-auth data endpoints are used here: the order book
snapshot. This is needed (a) on every WS (re)connect to seed/reseed local
book state before trusting incremental updates, and (b) optionally during
universe refinement to rank candidates on real spread/depth.
"""
from __future__ import annotations

from dataclasses import dataclass

import httpx

from .ratelimit import AsyncRateLimiter


@dataclass
class BookSnapshot:
    token_id: str
    bids: list[tuple[float, float]]   # (price, size), best first
    asks: list[tuple[float, float]]
    timestamp: str | None
    raw: dict                          # keep the raw payload for archival

    @property
    def spread(self) -> float | None:
        if not self.bids or not self.asks:
            return None
        return self.asks[0][0] - self.bids[0][0]

    @property
    def top_depth(self) -> float:
        b = self.bids[0][1] if self.bids else 0.0
        a = self.asks[0][1] if self.asks else 0.0
        return b + a


def _levels(side: list[dict]) -> list[tuple[float, float]]:
    out = []
    for lvl in side or []:
        try:
            out.append((float(lvl["price"]), float(lvl["size"])))
        except (KeyError, ValueError, TypeError):
            continue
    return out


class ClobClient:
    def __init__(self, base_url: str, limiter: AsyncRateLimiter,
                 client: httpx.AsyncClient) -> None:
        self._base = base_url.rstrip("/")
        self._limiter = limiter
        self._http = client

    async def book(self, token_id: str) -> BookSnapshot | None:
        await self._limiter.acquire()
        try:
            resp = await self._http.get(
                f"{self._base}/book", params={"token_id": token_id}
            )
            resp.raise_for_status()
        except httpx.HTTPError:
            return None
        data = resp.json()
        bids = _levels(data.get("bids", []))
        asks = _levels(data.get("asks", []))
        # Gamma/CLOB return bids ascending sometimes; ensure best-first.
        bids.sort(key=lambda x: x[0], reverse=True)
        asks.sort(key=lambda x: x[0])
        return BookSnapshot(
            token_id=token_id,
            bids=bids,
            asks=asks,
            timestamp=str(data.get("timestamp")) if data.get("timestamp") else None,
            raw=data,
        )
