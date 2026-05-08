"""KIS REST client — search-stock-info wrapper.

DAG 가 매일 04:00 KST 호출. dim_symbol 일배치에 사용.
Phase 1 단순화: 매 호출마다 새 OAuth token 발급 (다중 종목 시 첫 호출에만).
다중 종목 = 종목당 1회 REST. 토큰 throttle (1분당 1회) 고려해 호출 간 sleep 도 caller 책임.
"""
from __future__ import annotations

import os

import requests


class KisRestClient:
    def __init__(self, *, app_key: str, app_secret: str, base_url: str):
        self._app_key = app_key
        self._app_secret = app_secret
        self._base_url = base_url.rstrip("/")
        self._token: str | None = None

    @classmethod
    def from_env(cls) -> "KisRestClient":
        return cls(
            app_key=os.environ["KIS_APP_KEY"],
            app_secret=os.environ["KIS_APP_SECRET"],
            base_url=os.environ.get("KIS_BASE_URL", "https://openapi.koreainvestment.com:9443"),
        )

    def _ensure_token(self) -> None:
        if self._token:
            return
        r = requests.post(
            f"{self._base_url}/oauth2/tokenP",
            timeout=10,
            json={
                "grant_type": "client_credentials",
                "appkey": self._app_key,
                "appsecret": self._app_secret,
            },
        )
        r.raise_for_status()
        self._token = r.json()["access_token"]

    def get_stock_info(self, symbol: str) -> dict:
        """KIS search-stock-info (TR_ID: CTPF1604R) — 종목 마스터 조회."""
        self._ensure_token()
        r = requests.get(
            f"{self._base_url}/uapi/domestic-stock/v1/quotations/search-stock-info",
            timeout=10,
            params={"PDNO": symbol, "PRDT_TYPE_CD": "300"},
            headers={
                "authorization": f"Bearer {self._token}",
                "appkey": self._app_key,
                "appsecret": self._app_secret,
                "tr_id": "CTPF1604R",
                "custtype": "P",
            },
        )
        r.raise_for_status()
        body = r.json()
        if body.get("rt_cd") != "0":
            raise RuntimeError(f"KIS error symbol={symbol}: {body.get('msg1')}")
        return body.get("output", {})
