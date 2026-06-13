from conftest import run
from pmrec.clob import BookSnapshot
from pmrec.config import UniverseConfig
from pmrec.gamma import MarketInfo
from pmrec.universe import UniverseManager, diff_universe, _seconds_to_end


def _market(cid, vol, tokens, end=None):
    return MarketInfo(condition_id=cid, question=cid, token_ids=tokens,
                      volume_24h=vol, liquidity=0.0, end_date=end)


class StubGamma:
    def __init__(self, markets):
        self.markets = markets

    async def top_markets(self, limit, *, min_volume_24h=0.0, min_liquidity=0.0):
        return self.markets[:limit]


class StubClob:
    """Returns a configured book per token id; None means 'no book'."""
    def __init__(self, books):
        self.books = books

    async def book(self, token_id):
        return self.books.get(token_id)


def _book(bid, ask, size=10.0):
    return BookSnapshot("t", bids=[(bid, size)], asks=[(ask, size)],
                        timestamp=None, raw={})


def test_diff_universe():
    add, remove = diff_universe({"a", "b"}, {"b", "c"})
    assert add == {"c"} and remove == {"a"}


def test_seconds_to_end_parsing():
    assert _seconds_to_end(None) is None
    assert _seconds_to_end("garbage") is None
    assert _seconds_to_end("2000-01-01T00:00:00Z") < 0      # in the past


def test_select_without_refinement_takes_top_by_volume():
    markets = [_market("m1", 100, ["a1", "a2"]), _market("m2", 50, ["b1", "b2"])]
    cfg = UniverseConfig(target_size=1, refine_with_book_depth=False)
    um = UniverseManager(cfg, StubGamma(markets), StubClob({}))
    tokens, mapping = run(um.select())
    assert tokens == {"a1", "a2"}
    assert mapping["a1"].condition_id == "m1"


def test_refine_prefers_tight_spread_over_raw_volume():
    # m_wide has more volume but a very wide book; m_tight has less volume but a
    # tight, deep book. Refinement should pick the tight one.
    m_wide = _market("wide", 1000.0, ["w"])
    m_tight = _market("tight", 200.0, ["t"])
    books = {"w": _book(0.10, 0.90), "t": _book(0.49, 0.51)}
    cfg = UniverseConfig(target_size=1, refine_with_book_depth=True)
    um = UniverseManager(cfg, StubGamma([m_wide, m_tight]), StubClob(books))
    tokens, _ = run(um.select())
    assert tokens == {"t"}


def test_refine_ranks_booked_above_bookless():
    m_booked = _market("booked", 10.0, ["b"])
    m_none = _market("none", 10_000.0, ["n"])        # huge volume, no book
    books = {"b": _book(0.49, 0.51), "n": None}
    cfg = UniverseConfig(target_size=1, refine_with_book_depth=True)
    um = UniverseManager(cfg, StubGamma([m_booked, m_none]), StubClob(books))
    tokens, _ = run(um.select())
    assert tokens == {"b"}


def test_resolution_horizon_filters_soon_to_close():
    soon = _market("soon", 100.0, ["s"], end="2000-01-01T00:00:00Z")
    later = _market("later", 50.0, ["l"], end="2999-01-01T00:00:00Z")
    cfg = UniverseConfig(target_size=5, refine_with_book_depth=False,
                         min_seconds_to_resolution=3600)
    um = UniverseManager(cfg, StubGamma([soon, later]), StubClob({}))
    tokens, _ = run(um.select())
    assert tokens == {"l"}
