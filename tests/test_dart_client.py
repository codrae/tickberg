"""DART OpenDART list.json wrapper — corp/symbol 필터링 + 에러 처리."""
from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from pipelines.bronze.dart_client import DartClient, DartError


def _ok(rows: list[dict]) -> MagicMock:
    resp = MagicMock()
    resp.json.return_value = {
        "status": "000",
        "page_no": 1,
        "total_page": 1,
        "list": rows,
    }
    resp.raise_for_status = lambda: None
    return resp


def test_fetch_disclosures_filters_by_stock_codes():
    http = MagicMock()
    http.get.return_value = _ok([
        {"rcept_no": "1", "stock_code": "005930", "report_nm": "분기보고서", "rcept_dt": "20260508"},
        {"rcept_no": "2", "stock_code": "999999", "report_nm": "기타", "rcept_dt": "20260508"},
    ])
    c = DartClient(api_key="K", http=http)

    rows = c.fetch_disclosures(business_date="20260508", stock_codes={"005930"})

    assert len(rows) == 1
    assert rows[0]["stock_code"] == "005930"


def test_fetch_disclosures_raises_on_api_error():
    http = MagicMock()
    bad = MagicMock()
    bad.json.return_value = {"status": "010", "message": "API key invalid"}
    bad.raise_for_status = lambda: None
    http.get.return_value = bad
    c = DartClient(api_key="K", http=http)

    with pytest.raises(DartError):
        c.fetch_disclosures(business_date="20260508", stock_codes={"005930"})


def test_fetch_disclosures_status_013_no_data():
    """DART 의 status='013' 는 '데이터 없음' — 정상 (빈 리스트 반환)."""
    http = MagicMock()
    resp = MagicMock()
    resp.json.return_value = {"status": "013", "message": "조회된 데이터 없음", "list": []}
    resp.raise_for_status = lambda: None
    http.get.return_value = resp
    c = DartClient(api_key="K", http=http)

    rows = c.fetch_disclosures(business_date="20260508", stock_codes={"005930"})

    assert rows == []


def test_fetch_disclosures_paginates():
    """page_no < total_page 면 다음 페이지 호출."""
    http = MagicMock()
    page1 = MagicMock()
    page1.json.return_value = {
        "status": "000", "page_no": 1, "total_page": 2,
        "list": [{"rcept_no": "1", "stock_code": "005930", "rcept_dt": "20260508"}],
    }
    page1.raise_for_status = lambda: None
    page2 = MagicMock()
    page2.json.return_value = {
        "status": "000", "page_no": 2, "total_page": 2,
        "list": [{"rcept_no": "2", "stock_code": "005930", "rcept_dt": "20260508"}],
    }
    page2.raise_for_status = lambda: None
    http.get.side_effect = [page1, page2]
    c = DartClient(api_key="K", http=http)

    rows = c.fetch_disclosures(business_date="20260508", stock_codes={"005930"})

    assert len(rows) == 2
    assert http.get.call_count == 2
