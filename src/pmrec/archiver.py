"""Raw frame archiver.

The recorder's source of truth. Every frame received from the WebSocket (and
every REST snapshot taken on connect, and every connection lifecycle event) is
written here verbatim, append-only, with a local receipt timestamp. Parsing /
normalisation happens downstream off this immutable log, so a parser bug is
always replayable.

Frames are written newline-delimited JSON to a current file in spool_dir. On
rotation the current file is moved to pending_dir, where the S3 flusher picks
it up. Capture never blocks on disk or S3: shards push onto an asyncio.Queue
and a single writer task drains it.
"""
from __future__ import annotations

import asyncio
import json
import os
import time
from datetime import datetime, timezone


def _now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S_%fZ")


class RawArchiver:
    def __init__(self, spool_dir: str, pending_dir: str,
                 rotate_interval_s: int, rotate_max_bytes: int,
                 queue_maxsize: int = 100_000) -> None:
        self.spool_dir = spool_dir
        self.pending_dir = pending_dir
        self.rotate_interval_s = rotate_interval_s
        self.rotate_max_bytes = rotate_max_bytes
        os.makedirs(spool_dir, exist_ok=True)
        os.makedirs(pending_dir, exist_ok=True)

        self.queue: asyncio.Queue[dict] = asyncio.Queue(maxsize=queue_maxsize)
        self._fh = None
        self._path = ""
        self._opened_at = 0.0
        self._bytes = 0
        self._stop = asyncio.Event()

    def record(self, source: str, shard_id: int, payload: dict) -> None:
        """Non-blocking enqueue. Called from capture shards.

        Drops with a stderr warning if the queue is full rather than blocking
        the socket read loop. Tune queue_maxsize / writer throughput if you
        ever see drops.
        """
        rec = {
            "recv_ts": time.time(),          # local receipt time (epoch seconds)
            "recv_iso": _now_iso(),
            "source": source,               # "ws" | "rest_snapshot" | "lifecycle"
            "shard": shard_id,
            "payload": payload,
        }
        try:
            self.queue.put_nowait(rec)
        except asyncio.QueueFull:
            print("WARN: archive queue full, dropping frame", flush=True)

    async def run(self) -> None:
        self._open_new()
        try:
            while not self._stop.is_set():
                try:
                    rec = await asyncio.wait_for(self.queue.get(), timeout=1.0)
                except asyncio.TimeoutError:
                    self._maybe_rotate()
                    continue
                line = json.dumps(rec, separators=(",", ":")) + "\n"
                data = line.encode()
                self._fh.write(data)
                self._bytes += len(data)
                self._maybe_rotate()
        finally:
            self._close_and_promote()

    def stop(self) -> None:
        self._stop.set()

    # -- internals ---------------------------------------------------------
    def _open_new(self) -> None:
        name = f"frames_{_now_iso()}.jsonl"
        self._path = os.path.join(self.spool_dir, name)
        self._fh = open(self._path, "wb", buffering=1024 * 1024)
        self._opened_at = time.monotonic()
        self._bytes = 0

    def _maybe_rotate(self) -> None:
        age = time.monotonic() - self._opened_at
        if age >= self.rotate_interval_s or self._bytes >= self.rotate_max_bytes:
            self._close_and_promote()
            self._open_new()

    def _close_and_promote(self) -> None:
        if self._fh is None:
            return
        self._fh.flush()
        os.fsync(self._fh.fileno())
        self._fh.close()
        self._fh = None
        if self._bytes == 0:
            os.remove(self._path)         # nothing written, don't ship empty
            return
        dest = os.path.join(self.pending_dir, os.path.basename(self._path))
        os.replace(self._path, dest)
