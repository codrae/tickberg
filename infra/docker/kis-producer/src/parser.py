"""KIS H0STCNT0 (국내주식 실시간체결가 KRX) payload parser.

실제 KIS WebSocket frame 구조 (한국투자증권 open-trading-api `ccnl_krx` 참조):

    0|H0STCNT0|<N>|<r1_field0>^<r1_field1>^...^<r1_field45>^<r2_field0>^...

- 0 = 암호화 플래그 (0=평문, 1=암호화)
- H0STCNT0 = TR_ID
- <N> = 레코드 개수 (3자리 zero-padded, ex: "001", "004")
- 그 다음에 N 개의 레코드가 각각 46 개 필드 (^-구분) 로 직렬화되어 이어짐
  → 총 ^-필드 수 = N * 46

레코드 필드 (KIS 공식 column 순서, index 0..45):
    0  MKSC_SHRN_ISCD            종목코드
    1  STCK_CNTG_HOUR            체결시간 HHMMSS
    2  STCK_PRPR                 체결가
    ...
    10 ASKP1                     매도호가1
    11 BIDP1                     매수호가1
    12 CNTG_VOL                  체결량
    13 ACML_VOL                  누적거래량
    14 ACML_TR_PBMN              누적거래대금
    ...
    21 CCLD_DVSN                 체결구분 ('1'=매수, '5'=매도, 그 외=알수없음)
    ...
    45 VI_STND_PRC               정적VI발동기준가

Phase 1 추출 컬럼: symbol, trade_ts_kst, price, trade_side, volume,
best_ask_price, best_bid_price, cum_volume, cum_amount, raw_payload.
"""
from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from typing import Any

_TR_ID = "H0STCNT0"
_HEADER_PARTS = 4
_FIELDS_PER_RECORD = 46
_REQUIRED_INDEX = 21  # 가장 오른쪽 사용 컬럼 (CCLD_DVSN)


class ParseError(ValueError):
    """KIS payload parse 실패. raw text 보존하고 caller 가 dead-letter 처리."""


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


def _normalize_side(raw: str) -> str | None:
    if raw == "1":
        return "+"
    if raw == "5":
        return "-"
    return None


def _parse_record(fields: list[str], business_day: date) -> dict[str, Any]:
    symbol = fields[0]
    hhmmss = fields[1]
    if len(hhmmss) != 6 or not hhmmss.isdigit():
        raise ParseError(f"trade_time invalid: {hhmmss!r}")

    trade_dt = datetime(
        business_day.year, business_day.month, business_day.day,
        int(hhmmss[0:2]), int(hhmmss[2:4]), int(hhmmss[4:6]),
    )
    return {
        "symbol": symbol,
        "trade_ts_kst": trade_dt,
        "price": _parse_decimal_positive(fields[2], "price"),
        "trade_side": _normalize_side(fields[21]),
        "volume": _parse_int_nonneg(fields[12], "volume"),
        "best_ask_price": _parse_decimal_positive(fields[10], "best_ask_price"),
        "best_bid_price": _parse_decimal_positive(fields[11], "best_bid_price"),
        "cum_volume": _parse_int_nonneg(fields[13], "cum_volume"),
        "cum_amount": _parse_int_nonneg(fields[14], "cum_amount"),
        "raw_payload": "^".join(fields),
    }


def parse_h0stcnt0(payload: str, *, business_day: date) -> list[dict[str, Any]]:
    """KIS H0STCNT0 frame → list of dict rows. 비-H0STCNT0 frame 은 ParseError."""
    line = payload.strip()
    if not line:
        return []

    parts = line.split("|", 3)
    if len(parts) < _HEADER_PARTS:
        raise ParseError(f"header expects {_HEADER_PARTS} parts, got {len(parts)}")

    if parts[1] != _TR_ID:
        raise ParseError(f"unexpected tr_id: {parts[1]!r}")

    try:
        n_records = int(parts[2])
    except ValueError as e:
        raise ParseError(f"bad record count: {parts[2]!r}") from e

    body_fields = parts[3].split("^")
    expected = n_records * _FIELDS_PER_RECORD
    if len(body_fields) < expected:
        raise ParseError(
            f"body expects {expected} fields ({n_records}*{_FIELDS_PER_RECORD}), "
            f"got {len(body_fields)}"
        )

    rows: list[dict[str, Any]] = []
    for i in range(n_records):
        start = i * _FIELDS_PER_RECORD
        end = start + _FIELDS_PER_RECORD
        rows.append(_parse_record(body_fields[start:end], business_day))
    return rows
