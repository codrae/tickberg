from datetime import date
from decimal import Decimal

import pytest

from src.parser import ParseError, parse_h0stcnt0
from tests.fixtures.kis_h0stcnt0_samples import (
    SAMPLE_BUY_SIDE,
    SAMPLE_INVALID_TR_ID,
    SAMPLE_MULTI_RECORD,
    SAMPLE_NEGATIVE_PRICE,
    SAMPLE_NORMAL,
    SAMPLE_SELL_SIDE,
    SAMPLE_UNKNOWN_SIDE,
)


def test_parses_single_record_real_format():
    rows = parse_h0stcnt0(SAMPLE_NORMAL, business_day=date(2026, 5, 8))
    assert len(rows) == 1
    r = rows[0]
    assert r["symbol"] == "005930"
    assert r["price"] == Decimal("266000")
    assert r["volume"] == 208
    assert r["cum_volume"] == 8607809
    assert r["cum_amount"] == 2261385079750
    assert r["best_ask_price"] == Decimal("266500")
    assert r["best_bid_price"] == Decimal("266000")
    assert r["trade_ts_kst"].isoformat() == "2026-05-08T10:05:23"


def test_negative_price_rejected():
    with pytest.raises(ParseError):
        parse_h0stcnt0(SAMPLE_NEGATIVE_PRICE, business_day=date(2026, 5, 8))


def test_trade_side_buy_normalized_to_plus():
    rows = parse_h0stcnt0(SAMPLE_BUY_SIDE, business_day=date(2026, 5, 8))
    assert rows[0]["trade_side"] == "+"


def test_trade_side_sell_normalized_to_minus():
    rows = parse_h0stcnt0(SAMPLE_SELL_SIDE, business_day=date(2026, 5, 8))
    assert rows[0]["trade_side"] == "-"


def test_trade_side_unknown_normalized_to_none():
    rows = parse_h0stcnt0(SAMPLE_UNKNOWN_SIDE, business_day=date(2026, 5, 8))
    assert rows[0]["trade_side"] is None


def test_multi_record_frame_yields_all_records():
    rows = parse_h0stcnt0(SAMPLE_MULTI_RECORD, business_day=date(2026, 5, 8))
    assert len(rows) == 3
    assert [r["symbol"] for r in rows] == ["005930", "000660", "035420"]
    assert [r["price"] for r in rows] == [Decimal("266000"), Decimal("1648000"), Decimal("211500")]


def test_invalid_tr_id_rejected():
    with pytest.raises(ParseError):
        parse_h0stcnt0(SAMPLE_INVALID_TR_ID, business_day=date(2026, 5, 8))


def test_raw_payload_preserves_record_text():
    rows = parse_h0stcnt0(SAMPLE_NORMAL, business_day=date(2026, 5, 8))
    raw = rows[0]["raw_payload"]
    assert raw.startswith("005930^100523^266000^")
    assert raw.count("^") == 45
