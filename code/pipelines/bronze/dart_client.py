"""DART OpenDART list.json wrapper.

DART OpenDART API (https://opendart.fss.or.kr) 의 list.json 호출.
일배치 06:00 KST 가 영업일 전날 공시 list 수집 후 stock_code 로 필터링.

urllib 표준 라이브러리 사용 — Spark 컨테이너에 requests 의존성 없음.
테스트 시 requests.Session-like mock 주입 가능 (.get() → .json(), .raise_for_status()).
"""
from __future__ import annotations

import json
from collections.abc import Iterable
from typing import Any
from urllib import parse as urllib_parse
from urllib import request as urllib_request


class DartError(RuntimeError):
    pass


class _UrllibResponse:
    """requests.Response 와 인터페이스 호환되는 얇은 wrapper."""

    def __init__(self, body: bytes, status: int):
        self._body = body
        self._status = status

    def json(self) -> Any:
        return json.loads(self._body.decode("utf-8"))

    def raise_for_status(self) -> None:
        if 400 <= self._status < 600:
            raise DartError(f"DART HTTP {self._status}")


class _UrllibSession:
    """requests.Session 의 .get() 인터페이스만 stdlib urllib 로 구현."""

    def get(self, url: str, params: dict | None = None, timeout: int = 15) -> _UrllibResponse:
        full = f"{url}?{urllib_parse.urlencode(params or {})}"
        with urllib_request.urlopen(full, timeout=timeout) as r:
            return _UrllibResponse(r.read(), r.status)


class DartClient:
    BASE = "https://opendart.fss.or.kr/api"

    def __init__(self, *, api_key: str, http: Any = None):
        """http: requests.Session 호환 객체. None 시 stdlib urllib 사용."""
        self._api_key = api_key
        self._http = http or _UrllibSession()

    def fetch_disclosures(
        self, *, business_date: str, stock_codes: Iterable[str]
    ) -> list[dict[str, Any]]:
        """`business_date` (YYYYMMDD) 의 모든 공시 list 를 받아 stock_codes 로 필터링.

        DART list.json 응답:
          status: "000" = 정상, "013" = 데이터 없음 (정상 — 영업일 휴장 등), 기타 = 에러
          page_no / total_page: 페이지네이션
          list: [{rcept_no, corp_code, corp_name, stock_code, report_nm, rcept_dt, flr_nm, rm}]
        """
        wanted = set(stock_codes)
        all_rows: list[dict[str, Any]] = []
        page = 1
        while True:
            r = self._http.get(
                f"{self.BASE}/list.json",
                params={
                    "crtfc_key": self._api_key,
                    "bgn_de": business_date,
                    "end_de": business_date,
                    "page_no": page,
                    "page_count": 100,
                },
                timeout=15,
            )
            r.raise_for_status()
            body = r.json()
            status = body.get("status")
            if status == "013":  # no data
                break
            if status != "000":
                raise DartError(
                    f"DART error status={status} msg={body.get('message')}"
                )
            for row in body.get("list", []):
                if row.get("stock_code") in wanted:
                    all_rows.append(row)
            if body.get("page_no", page) >= body.get("total_page", page):
                break
            page += 1
        return all_rows
