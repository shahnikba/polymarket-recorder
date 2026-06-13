"""S3 flusher.

Periodically scans pending_dir for rotated frame files, gzips and uploads each
to S3 under a date-partitioned key, then deletes the local copy on success.
Runs on its own timer (this is the only "every few minutes to S3" part of the
system; capture itself is continuous).

boto3 is synchronous, so uploads run in a thread executor to avoid blocking the
event loop.
"""
from __future__ import annotations

import asyncio
import gzip
import os
import shutil
from datetime import datetime, timezone

import boto3


class S3Flusher:
    def __init__(self, pending_dir: str, bucket: str, prefix: str,
                 flush_interval_s: int, region: str | None,
                 delete_after_upload: bool) -> None:
        self.pending_dir = pending_dir
        self.bucket = bucket
        self.prefix = prefix.strip("/")
        self.flush_interval_s = flush_interval_s
        self.delete_after_upload = delete_after_upload
        self._s3 = boto3.client("s3", region_name=region)
        self._stop = asyncio.Event()

    async def run(self) -> None:
        while not self._stop.is_set():
            try:
                await asyncio.to_thread(self._flush_once)
            except Exception as e:                      # noqa: BLE001
                print(f"WARN: flush error: {e}", flush=True)
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=self.flush_interval_s)
            except asyncio.TimeoutError:
                pass
        # final drain on shutdown
        await asyncio.to_thread(self._flush_once)

    def stop(self) -> None:
        self._stop.set()

    # -- internals ---------------------------------------------------------
    def _flush_once(self) -> None:
        if self.bucket == "CHANGE_ME":
            return  # not configured yet; leave files in pending_dir
        for fname in sorted(os.listdir(self.pending_dir)):
            if not fname.endswith(".jsonl"):
                continue
            src = os.path.join(self.pending_dir, fname)
            gz = src + ".gz"
            with open(src, "rb") as fi, gzip.open(gz, "wb") as fo:
                shutil.copyfileobj(fi, fo)
            key = self._key_for(fname)
            self._s3.upload_file(gz, self.bucket, key)
            os.remove(gz)
            if self.delete_after_upload:
                os.remove(src)
            print(f"uploaded s3://{self.bucket}/{key}", flush=True)

    def _key_for(self, fname: str) -> str:
        # Partition by UTC date for cheap Athena/DuckDB scanning later.
        day = datetime.now(timezone.utc).strftime("%Y/%m/%d")
        return f"{self.prefix}/{day}/{fname}.gz"
