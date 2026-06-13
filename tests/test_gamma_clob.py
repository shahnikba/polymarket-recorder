import httpx

from conftest import mock_client, run
from pmrec.clob import ClobClient, BookSnapshot, _levels
from pmrec.gamma import GammaClient, _parse_token_ids
from pmrec.ratelimit import AsyncRateLimiter


def _limiter():
    return AsyncRateLimiter(0.0)


# -- gamma ----------------------------------------------------------------
def test_parse_token_ids_variants():
    assert _parse_token_ids({"clobTokenIds": '["1","2"]'}) == ["1", "2"]
    assert _parse_token_ids({"clobTokenIds": [1, 2]}) == ["1", "2"]
    assert _parse_token_ids({"clob_token_ids": ["9"]}) == ["9"]
    assert _parse_token_ids({}) == []
    assert _parse_token_ids({"clobTokenIds": "not json"}) == []


def test_top_markets_parses_and_skips_tokenless():
    body = [
        {"conditionId": "0xabc", "question": "Q1", "clobTokenIds": '["t1","t2"]',
         "volume24hr": 1234.5, "liquidityNum": 50.0, "endDate": "2030-01-01T00:00:00Z"},
        {"conditionId": "0xdef", "question": "no tokens", "volume24hr": 9.0},
    ]
    seen = {}

    def handler(req: httpx.Request) -> httpx.Response:
        seen["url"] = str(req.url)
        return httpx.Response(200, json=body)

    client = mock_client(handler)
    g = GammaClient("https://gamma-api.polymarket.com", _limiter(), client)
    out = run(g.top_markets(200, min_volume_24h=100))
    run(client.aclose())

    assert "order=volume24hr" in seen["url"] and "volume_num_min=100" in seen["url"]
    assert len(out) == 1                       # tokenless market dropped
    m = out[0]
    assert m.token_ids == ["t1", "t2"]
    assert m.volume_24h == 1234.5 and m.liquidity == 50.0


# -- clob -----------------------------------------------------------------
def test_levels_skips_malformed():
    levels = _levels([{"price": "0.5", "size": "10"}, {"price": "x"}, {"size": "1"}])
    assert levels == [(0.5, 10.0)]


def test_book_snapshot_spread_and_depth():
    snap = BookSnapshot("t", bids=[(0.40, 5.0)], asks=[(0.45, 7.0)],
                        timestamp=None, raw={})
    assert round(snap.spread, 3) == 0.05
    assert snap.top_depth == 12.0
    one_sided = BookSnapshot("t", bids=[], asks=[(0.45, 7.0)], timestamp=None, raw={})
    assert one_sided.spread is None


def test_book_sorts_best_first():
    body = {"bids": [{"price": "0.30", "size": "1"}, {"price": "0.40", "size": "2"}],
            "asks": [{"price": "0.60", "size": "3"}, {"price": "0.50", "size": "4"}],
            "timestamp": "123"}

    def handler(req: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=body)

    client = mock_client(handler)
    c = ClobClient("https://clob.polymarket.com", _limiter(), client)
    snap = run(c.book("tok"))
    run(client.aclose())

    assert snap.bids[0] == (0.40, 2.0)         # best bid first (highest price)
    assert snap.asks[0] == (0.50, 4.0)         # best ask first (lowest price)
    assert snap.timestamp == "123"


def test_book_returns_none_on_http_error():
    def handler(req: httpx.Request) -> httpx.Response:
        return httpx.Response(503)

    client = mock_client(handler)
    c = ClobClient("https://clob.polymarket.com", _limiter(), client)
    assert run(c.book("tok")) is None
    run(client.aclose())
