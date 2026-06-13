import asyncio
import json
import time

from conftest import run
from pmrec.archiver import RawArchiver
from pmrec.ratelimit import AsyncRateLimiter


def test_ratelimiter_enforces_min_interval():
    lim = AsyncRateLimiter(0.05)

    async def go():
        t0 = time.monotonic()
        await lim.acquire()
        await lim.acquire()
        await lim.acquire()
        return time.monotonic() - t0

    elapsed = run(go())
    assert elapsed >= 0.10           # two gaps of >=0.05s between three calls


def test_archiver_writes_rotates_and_promotes(tmp_path):
    spool = tmp_path / "spool"
    pending = tmp_path / "pending"
    arch = RawArchiver(str(spool), str(pending),
                       rotate_interval_s=10_000, rotate_max_bytes=10_000)

    async def go():
        task = asyncio.create_task(arch.run())
        await asyncio.sleep(0.02)
        arch.record("ws", 0, {"hello": "world"})
        arch.record("lifecycle", 1, {"event": "connect"})
        await asyncio.sleep(0.05)
        arch.stop()
        await task

    run(go())

    # On stop the open file is flushed and promoted to pending_dir.
    files = list(pending.glob("*.jsonl"))
    assert len(files) == 1
    lines = files[0].read_text().splitlines()
    assert len(lines) == 2
    rec = json.loads(lines[0])
    assert rec["source"] == "ws" and rec["payload"] == {"hello": "world"}
    assert "recv_ts" in rec and "recv_iso" in rec


def test_archiver_drops_empty_file(tmp_path):
    arch = RawArchiver(str(tmp_path / "spool"), str(tmp_path / "pending"),
                       rotate_interval_s=10_000, rotate_max_bytes=10_000)

    async def go():
        task = asyncio.create_task(arch.run())
        await asyncio.sleep(0.02)
        arch.stop()
        await task

    run(go())
    assert list((tmp_path / "pending").glob("*.jsonl")) == []   # nothing shipped
