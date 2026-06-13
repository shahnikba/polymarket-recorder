"""Universe manager.

Decides *which* markets to record so you don't capture the whole platform.
Two-stage selection:

  1. Cheap shortlist: Gamma, top-N active markets by 24h volume (one request).
  2. Optional refinement: fetch CLOB book snapshots for the shortlist and rank
     on real microstructure quality (tight spread + top-of-book depth) rather
     than the Gamma 'liquidity' estimate, then keep the top `target_size`.

Returns the target set of CLOB token ids. The orchestrator diffs this against
what's currently subscribed and applies add/remove deltas to the shards, so the
live universe tracks rotating activity (e.g. World Cup markets aging out) with
no code change.
"""
from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass
from datetime import datetime, timezone

from .clob import ClobClient
from .config import UniverseConfig
from .gamma import GammaClient, MarketInfo


def _seconds_to_end(end_date: str | None) -> float | None:
    if not end_date:
        return None
    try:
        dt = datetime.fromisoformat(end_date.replace("Z", "+00:00"))
    except ValueError:
        return None
    return (dt - datetime.now(timezone.utc)).total_seconds()


@dataclass
class Scored:
    market: MarketInfo
    score: float


class UniverseManager:
    def __init__(self, cfg: UniverseConfig, gamma: GammaClient,
                 clob: ClobClient) -> None:
        self.cfg = cfg
        self.gamma = gamma
        self.clob = clob

    async def select(self) -> tuple[set[str], dict[str, MarketInfo]]:
        """Return (set_of_token_ids, token_id -> MarketInfo)."""
        candidates = await self.gamma.top_markets(
            self.cfg.candidate_limit,
            min_volume_24h=self.cfg.min_volume_24h,
            min_liquidity=self.cfg.min_liquidity,
        )

        # Drop markets resolving inside the horizon, if configured.
        if self.cfg.min_seconds_to_resolution > 0:
            kept = []
            for m in candidates:
                s = _seconds_to_end(m.end_date)
                if s is None or s >= self.cfg.min_seconds_to_resolution:
                    kept.append(m)
            candidates = kept

        if self.cfg.refine_with_book_depth and len(candidates) > self.cfg.target_size:
            chosen = await self._refine(candidates)
        else:
            chosen = candidates[: self.cfg.target_size]

        token_ids: set[str] = set()
        token_to_market: dict[str, MarketInfo] = {}
        for m in chosen:
            for tid in m.token_ids:
                token_ids.add(tid)
                token_to_market[tid] = m
        return token_ids, token_to_market

    async def _refine(self, candidates: list[MarketInfo]) -> list[MarketInfo]:
        """Rank candidates by real spread/depth from the CLOB book.

        Uses the Yes-token (first token id) as the representative leg. The score
        below is a sensible default that selects on *tradeable* liquidity rather
        than headline volume; the three exponents are the knobs to turn for your
        research definition of liquidity.

            score = volume_24h**0.5  *  depth**0.25  /  spread

          - sqrt(volume): rewards activity but with diminishing returns, so one
            mega-volume market doesn't dominate the whole universe.
          - depth**0.25: a gentle bonus for thicker top-of-book.
          - / spread: the main discriminator — tight two-sided books (the ones
            worth recording at frame resolution) score far above wide ones.
            Spread is floored so a near-zero spread can't blow the score up.

        Markets with no two-sided book fall back to a volume-only score scaled
        well below any booked market, so they sort under everything tradeable
        but still keep their relative volume ordering instead of collapsing to a
        tie at zero.
        """
        async def score(m: MarketInfo) -> Scored:
            snap = await self.clob.book(m.token_ids[0]) if m.token_ids else None
            if snap is None or snap.spread is None:
                # No book / one-sided: rank under any booked market, but keep
                # the volume signal so these still order sensibly among themselves.
                return Scored(m, score=m.volume_24h ** 0.5 * 1e-6)
            spread = max(snap.spread, 1e-4)
            depth = max(snap.top_depth, 1.0)
            s = (m.volume_24h ** 0.5) * (depth ** 0.25) / spread
            return Scored(m, score=s)

        scored = await asyncio.gather(*(score(m) for m in candidates))
        scored.sort(key=lambda x: x.score, reverse=True)
        return [s.market for s in scored[: self.cfg.target_size]]


def diff_universe(current: set[str], target: set[str]) -> tuple[set[str], set[str]]:
    """Return (to_add, to_remove)."""
    return target - current, current - target
