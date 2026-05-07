"""KIS H0STCNT0 (체결가) payload parser.

H0STCNT0 frame은 '|'-구분 텍스트. 한 frame에 여러 줄(레코드) 가능.
1차 발표 13 필드: 0=symbol(MKSC_SHRN_ISCD), 1=trade_time(HHMMSS),
2=price(STCK_PRPR), 3=trade_side(CCLD_DVSN: '+' buy / '-' sell / '' unknown),
4=volume(CNTG_VOL), 5=...spread, 6..9=ASKP1/BIDP1/BIDP2/BIDP3 등 (broker-specific),
10=cum_volume(ACML_VOL), 11=cum_amount(ACML_TR_PBMN), 12=trade_ratio.
운영 중 KIS docs와 mismatch 발견하면 fixture에 변종 추가하고 보강.
"""
from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from typing import Any


class ParseError(ValueError):
    """KIS payload parse 실패. raw text 보존하고 caller가 dead-letter 처리."""


_REQUIRED_FIELDS = 13


def _parse_decimal_positive(s: str, field: str) -> Decimal:
    try:
        v = Decimal(s)
    except InvalidOperation as e:
        raise ParseError(f"{field} not decimal: {s!r}") from e
    if v < 0:
        raise ParseError(f"{field} negative: {v}")
    return v


def _parse_int_nonneg(s: str, field: str) -> int:
    try:
        v = int(s)
    except ValueError as e:
        raise ParseError(f"{field} not int: {s!r}") from e
    if v < 0:
        raise ParseError(f"{field} negative: {v}")
    return v


def _parse_line(line: str, business_day: date) -> dict[str, Any]:
    parts = line.split("|")
    if len(parts) < _REQUIRED_FIELDS:
        raise ParseError(f"need >= {_REQUIRED_FIELDS} fields, got {len(parts)}")

    symbol = parts[0]
    hhmmss = parts[1]
    if len(hhmmss) != 6 or not hhmmss.isdigit():
        raise ParseError(f"trade_time invalid: {hhmmss!r}")

    trade_dt = datetime(
        business_day.year, business_day.month, business_day.day,
        int(hhmmss[0:2]), int(hhmmss[2:4]), int(hhmmss[4:6]),
    )
    side = parts[3] or None  # '' → None

    return {
        "symbol": symbol,
        "trade_ts_kst": trade_dt,
        "price": _parse_decimal_positive(parts[2], "price"),
        "trade_side": side,
        "volume": _parse_int_nonneg(parts[4], "volume"),
        "best_ask_price": _parse_decimal_positive(parts[6], "best_ask_price"),
        "best_bid_price": _parse_decimal_positive(parts[7], "best_bid_price"),
        "cum_volume": _parse_int_nonneg(parts[10], "cum_volume"),
        "cum_amount": _parse_int_nonneg(parts[11], "cum_amount"),
        "raw_payload": line,
    }


def parse_h0stcnt0(payload: str, *, business_day: date) -> list[dict[str, Any]]:
    """KIS H0STCNT0 frame → list of dict rows.

    Single frame may carry multiple newline-separated records.
    """
    rows: list[dict[str, Any]] = []
    for raw_line in payload.splitlines():
        line = raw_line.strip()
        if not line:
            continue
        rows.append(_parse_line(line, business_day))
    return rows
