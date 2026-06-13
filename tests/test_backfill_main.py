import json

import httpx

from conftest import mock_client, run
from pmrec.backfill import fetch_trades, _write_frames, _MAX_OFFSET, _MAX_LIMIT
from pmrec.main import _shard_for
from pmrec.ratelimit import AsyncRateLimiter


def test_shard_for_in_range_and_stable():
    for tid in ("123", "456", "0xabc"):
        s = _shard_for(tid, 4)
        assert 0 <= s < 4
        assert s == _shard_for(tid, 4)        # deterministic within a process


def test_fetch_trades_paginates_until_short_page():
    # Two full pages of `limit`, then a short final page -> stop.
    offsets = []

    def handler(req: httpx.Request) -> httpx.Response:
        off = int(req.url.params["offset"])
        lim = int(req.url.params["limit"])
        offsets.append(off)
        assert req.url.params["market"] == "0xmkt"
        assert req.url.params["takerOnly"] == "true"
        if off == 0:
            page = [{"i": i} for i in range(lim)]
        elif off == lim:
            page = [{"i": i} for i in range(lim)]
        else:
            page = [{"i": 0}]                 # short -> last page
        return httpx.Response(200, json=page)

    client = mock_client(handler)
    trades = run(fetch_trades(client, AsyncRateLimiter(0.0), "0xmkt", limit=2))
    run(client.aclose())

    assert offsets == [0, 2, 4]
    assert len(trades) == 5                    # 2 + 2 + 1


def test_fetch_trades_stops_on_empty_page():
    def handler(req: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=[])

    client = mock_client(handler)
    trades = run(fetch_trades(client, AsyncRateLimiter(0.0), "0xmkt", limit=10))
    run(client.aclose())
    assert trades == []


def test_fetch_trades_respects_offset_cap():
    # Always returns full pages so it would loop forever without the cap.
    calls = {"n": 0}

    def handler(req: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        lim = int(req.url.params["limit"])
        return httpx.Response(200, json=[{"i": i} for i in range(lim)])

    client = mock_client(handler)
    trades = run(fetch_trades(client, AsyncRateLimiter(0.0), "0xmkt", limit=_MAX_LIMIT))
    run(client.aclose())
    # Bounded by the offset cap, not infinite.
    assert calls["n"] == _MAX_OFFSET // _MAX_LIMIT + 1


def test_write_frames_envelope(tmp_path):
    trades = [{"timestamp": 1700000000, "side": "BUY", "price": "0.5"}]
    path = _write_frames(str(tmp_path), "0xmkt", trades)
    assert path is not None
    rec = json.loads(tmp_path.joinpath(path.split("/")[-1]).read_text().strip())
    assert rec["source"] == "backfill_trade"
    assert rec["shard"] == -1
    assert rec["market"] == "0xmkt"
    assert rec["recv_ts"] == 1700000000
    assert rec["payload"]["side"] == "BUY"


def test_write_frames_empty_returns_none(tmp_path):
    assert _write_frames(str(tmp_path), "0xmkt", []) is None
