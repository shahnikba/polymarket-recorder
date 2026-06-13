"""WebSocket capture shard.

One CaptureShard owns a single WS connection to the public market channel and
records a slice of the asset universe. The orchestrator runs several shards in
parallel so no single connection holds the whole universe.

Per-connection lifecycle:
  connect
    -> subscribe to the shard's asset_ids
    -> seed: REST GET /book snapshot per asset (rate-limited, archived)
    -> concurrently: ping loop (every ~10s) + receive loop (archive every frame)
  on disconnect / staleness:
    -> log a lifecycle 'disconnect' record (so gaps are explicit in the archive)
    -> backoff, reconnect, RE-SNAPSHOT before trusting incrementals again

Dynamic membership: add()/remove() update the shard's asset set and send
subscription-update messages without tearing down the connection. New assets
get a fresh snapshot.

NOTE on the ping wire format: Polymarket community clients commonly send the
literal text "PING". Some docs show an empty JSON object. If you see the server
dropping you, try sending "{}" instead — adjust _PING below.
"""
from __future__ import annotations

import asyncio
import json
import time

import websockets

from .archiver import RawArchiver
from .clob import ClobClient
from .config import CaptureConfig
from .ratelimit import AsyncRateLimiter

_PING = "PING"


class CaptureShard:
    def __init__(self, shard_id: int, cfg: CaptureConfig, ws_url: str,
                 clob: ClobClient, archiver: RawArchiver,
                 limiter: AsyncRateLimiter) -> None:
        self.shard_id = shard_id
        self.cfg = cfg
        self.ws_url = ws_url
        self.clob = clob
        self.archiver = archiver
        self.limiter = limiter

        self.assets: set[str] = set()
        self._pending_add: set[str] = set()
        self._pending_remove: set[str] = set()
        self._ws: websockets.WebSocketClientProtocol | None = None
        self._last_msg = 0.0
        self._stop = asyncio.Event()
        self._lock = asyncio.Lock()

    def initial_assets(self, assets: set[str]) -> None:
        self.assets = set(assets)

    async def add(self, assets: set[str]) -> None:
        async with self._lock:
            new = assets - self.assets
            self.assets |= new
            self._pending_add |= new
        await self._apply_subscription_updates()

    async def remove(self, assets: set[str]) -> None:
        async with self._lock:
            gone = assets & self.assets
            self.assets -= gone
            self._pending_remove |= gone
        await self._apply_subscription_updates()

    async def run(self) -> None:
        backoff = self.cfg.reconnect_base_s
        while not self._stop.is_set():
            try:
                await self._connect_and_capture()
                backoff = self.cfg.reconnect_base_s   # reset on clean exit
            except Exception as e:                     # noqa: BLE001
                self._lifecycle("disconnect", {"error": repr(e)})
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, self.cfg.reconnect_max_s)

    def stop(self) -> None:
        self._stop.set()

    # -- internals ---------------------------------------------------------
    async def _connect_and_capture(self) -> None:
        if not self.assets:
            await asyncio.sleep(1.0)        # nothing to do yet
            return
        async with websockets.connect(
            self.ws_url, ping_interval=None, max_size=None, open_timeout=15
        ) as ws:
            self._ws = ws
            self._lifecycle("connect", {"n_assets": len(self.assets)})
            await self._subscribe(sorted(self.assets))
            await self._seed_snapshots(list(self.assets))
            self._last_msg = time.monotonic()
            await asyncio.gather(
                self._recv_loop(ws),
                self._ping_loop(ws),
                self._stale_watchdog(ws),
            )

    async def _subscribe(self, asset_ids: list[str]) -> None:
        msg: dict = {"type": "market", "assets_ids": asset_ids}
        if self.cfg.custom_feature_enabled:
            msg["custom_feature_enabled"] = True
        await self._ws.send(json.dumps(msg))

    async def _seed_snapshots(self, asset_ids: list[str]) -> None:
        """REST book snapshot per asset on (re)connect, archived as source of
        truth before incrementals are trusted. Rate-limited globally."""
        for tid in asset_ids:
            snap = await self.clob.book(tid)
            if snap is not None:
                self.archiver.record("rest_snapshot", self.shard_id, snap.raw)

    async def _recv_loop(self, ws) -> None:
        async for raw in ws:
            self._last_msg = time.monotonic()
            if raw in ("PONG", "{}", ""):       # heartbeat noise, skip archive
                continue
            try:
                payload = json.loads(raw)
            except (json.JSONDecodeError, TypeError):
                payload = {"_unparsed": raw}
            self.archiver.record("ws", self.shard_id, payload)

    async def _ping_loop(self, ws) -> None:
        while True:
            await asyncio.sleep(self.cfg.ping_interval_s)
            await ws.send(_PING)

    async def _stale_watchdog(self, ws) -> None:
        while True:
            await asyncio.sleep(self.cfg.stale_after_s / 2)
            if time.monotonic() - self._last_msg > self.cfg.stale_after_s:
                self._lifecycle("stale_reconnect", {})
                await ws.close()
                return

    async def _apply_subscription_updates(self) -> None:
        ws = self._ws
        if ws is None:
            return
        async with self._lock:
            add, remove = self._pending_add, self._pending_remove
            self._pending_add, self._pending_remove = set(), set()
        try:
            if add:
                await ws.send(json.dumps(
                    {"operation": "subscribe", "assets_ids": sorted(add)}))
                await self._seed_snapshots(sorted(add))
            if remove:
                await ws.send(json.dumps(
                    {"operation": "unsubscribe", "assets_ids": sorted(remove)}))
        except Exception:                          # noqa: BLE001
            # Connection died mid-update; reconnect path will resubscribe the
            # full current self.assets set, so just drop these deltas.
            pass

    def _lifecycle(self, event: str, extra: dict) -> None:
        self.archiver.record(
            "lifecycle", self.shard_id,
            {"event": event, "ts": time.time(), **extra},
        )
