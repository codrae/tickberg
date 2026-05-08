"""KIS H0STCNT0 frame fixtures.

실제 frame 구조:
    0|H0STCNT0|<N>|<record1>^<record2>^...^<recordN>
- 0 = 암호화 플래그 (0=평문, 1=암호화)
- H0STCNT0 = TR_ID
- N = 레코드 개수 (3자리 zero-padded)
- 각 record 는 46 필드 ^-구분 (count * 46 필드 전체 ^-구분)

운영 로그 (2026-05-08 영업일 09~10시) 의 실제 frame 으로 fixture 작성.
"""
from __future__ import annotations

_FIELDS_PER_RECORD = 46


def _record(
    *,
    symbol: str = "005930",
    hhmmss: str = "100523",
    price: str = "266000",
    askp1: str = "266500",
    bidp1: str = "266000",
    cntg_vol: str = "208",
    acml_vol: str = "8607809",
    acml_tr_pbmn: str = "2261385079750",
    ccld_dvsn: str = "5",
) -> str:
    """46-field H0STCNT0 record. 사용하지 않는 필드는 0 으로 패딩."""
    fields = [
        symbol, hhmmss, price,
        "5", "-5500", "-2.03",      # PRDY_VRSS_SIGN, PRDY_VRSS, PRDY_CTRT
        "262712.48", "260000", "267500", "260000",   # WGHN_AVRG, OPRC, HGPR, LWPR
        askp1, bidp1, cntg_vol, acml_vol, acml_tr_pbmn,
        "30395", "105050", "74655", "146.32", "2609400", "3818023",
        ccld_dvsn,
    ]
    while len(fields) < _FIELDS_PER_RECORD:
        fields.append("0")
    assert len(fields) == _FIELDS_PER_RECORD
    return "^".join(fields)


def _frame(*records: str) -> str:
    return f"0|H0STCNT0|{len(records):03d}|" + "^".join(records)


SAMPLE_NORMAL = _frame(_record())

SAMPLE_BUY_SIDE = _frame(_record(ccld_dvsn="1"))
SAMPLE_SELL_SIDE = _frame(_record(ccld_dvsn="5"))
SAMPLE_UNKNOWN_SIDE = _frame(_record(ccld_dvsn="0"))

SAMPLE_NEGATIVE_PRICE = _frame(_record(price="-100"))

SAMPLE_MULTI_RECORD = _frame(
    _record(symbol="005930", hhmmss="100523", price="266000"),
    _record(symbol="000660", hhmmss="100524", price="1648000",
            askp1="1648500", bidp1="1647500"),
    _record(symbol="035420", hhmmss="100525", price="211500",
            askp1="212000", bidp1="211000"),
)

SAMPLE_INVALID_TR_ID = "0|H0STASP0|001|" + _record()
