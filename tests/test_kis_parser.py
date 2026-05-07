from datetime import date
from decimal import Decimal

import pytest

from src.parser import ParseError, parse_h0stcnt0
from tests.fixtures.kis_h0stcnt0_samples import (
    SAMPLE_MISSING_TRADE_SIDE, SAMPLE_MULTI_LINE, SAMPLE_NEGATIVE_PRICE,
    SAMPLE_NORMAL, SAMPLE_NULL_PADDED,
)


def test_parses_basic_fields():
    rows = parse_h0stcnt0(SAMPLE_NORMAL, business_day=date(2026, 5, 8))
    assert len(rows) == 1
    r = rows[0]
    assert r["symbol"] == "005930"
    assert r["price"] == Decimal("72500")
    assert r["volume"] == 100
    assert r["cum_volume"] == 1234567
    assert r["cum_amount"] == 89500000000
    assert r["best_ask_price"] == Decimal("72500")
    assert r["best_bid_price"] == Decimal("72400")
    assert r["trade_ts_kst"].isoformat() == "2026-05-08T09:30:01"
    assert r["trade_side"] == "+"


def test_negative_price_rejected():
    with pytest.raises(ParseError):
        parse_h0stcnt0(SAMPLE_NEGATIVE_PRICE, business_day=date(2026, 5, 8))


def test_null_trade_side_normalized_to_none():
    rows = parse_h0stcnt0(SAMPLE_MISSING_TRADE_SIDE, business_day=date(2026, 5, 8))
    assert rows[0]["trade_side"] is None


def test_multiline_payload_yields_multiple_rows():
    rows = parse_h0stcnt0(SAMPLE_MULTI_LINE, business_day=date(2026, 5, 8))
    assert len(rows) == 2
    assert {r["symbol"] for r in rows} == {"005930", "035420"}


def test_raw_payload_preserved():
    rows = parse_h0stcnt0(SAMPLE_NULL_PADDED, business_day=date(2026, 5, 8))
    assert rows[0]["raw_payload"] == SAMPLE_NULL_PADDED
