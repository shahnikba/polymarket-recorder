"""Gamma API client (read-only market discovery).

Public, no auth. Used to rank active markets by 24h volume and extract the
CLOB token ids we then subscribe to on the WebSocket.
"""
from __future__ import annotations

import json
from dataclasses import dataclass

import httpx

from .ratelimit import AsyncRateLimiter


@dataclass
class MarketInfo:
    condition_id: str
    question: str
    token_ids: list[str]       # CLOB token ids (Yes / No)
    volume_24h: float
    liquidity: float
    end_date: str | None


def _parse_token_ids(market: dict) -> list[str]:
    """clobTokenIds is sometimes a JSON-encoded string, sometimes a list."""
    raw = market.get("clobTokenIds") or market.get("clob_token_ids")
    if raw is None:
        return []
    if isinstance(raw, str):
        try:
            raw = json.loads(raw)
        except json.JSONDecodeError:
            return []
    return [str(t) for t in raw] if isinstance(raw, list) else []


class GammaClient:
    def __init__(self, base_url: str, limiter: AsyncRateLimiter,
                 client: httpx.AsyncClient) -> None:
        self._base = base_url.rstrip("/")
        self._limiter = limiter
        self._http = client

    async def top_markets(
        self,
        limit: int,
        *,
        min_volume_24h: float = 0.0,
        min_liquidity: float = 0.0,
    ) -> list[MarketInfo]:
        """Active, open markets sorted by 24h volume descending."""
        params = {
            "active": "true",
            "closed": "false",
            "order": "volume24hr",
            "ascending": "false",
            "limit": str(limit),
        }
        if min_volume_24h > 0:
            params["volume_num_min"] = str(min_volume_24h)
        if min_liquidity > 0:
            params["liquidity_num_min"] = str(min_liquidity)

        await self._limiter.acquire()
        resp = await self._http.get(f"{self._base}/markets", params=params)
        resp.raise_for_status()
        data = resp.json()

        out: list[MarketInfo] = []
        for m in data:
            token_ids = _parse_token_ids(m)
            if not token_ids:
                continue
            out.append(
                MarketInfo(
                    condition_id=str(m.get("conditionId") or m.get("condition_id") or ""),
                    question=m.get("question", ""),
                    token_ids=token_ids,
                    volume_24h=float(m.get("volume24hr") or 0.0),
                    liquidity=float(m.get("liquidityNum") or m.get("liquidity") or 0.0),
                    end_date=m.get("endDate") or m.get("end_date"),
                )
            )
        return out
