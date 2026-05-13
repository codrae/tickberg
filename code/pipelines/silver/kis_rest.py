"""KIS REST client — search-stock-info wrapper.

DAG 가 매일 04:00 KST 호출. dim_symbol 일배치에 사용.
Phase 1 단순화: 매 호출마다 새 OAuth token 발급 (다중 종목 시 첫 호출에만 token).
urllib 표준 라이브러리 사용 — Spark 컨테이너에 추가 의존성 없이 동작.
"""
from __future__ import annotations

import json
import os
from urllib import error as urllib_error
from urllib import parse as urllib_parse
from urllib import request as urllib_request


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

    def _post_json(self, path: str, payload: dict, headers: dict | None = None) -> dict:
        url = f"{self._base_url}{path}"
        body = json.dumps(payload).encode("utf-8")
        req = urllib_request.Request(url, data=body, method="POST")
        req.add_header("content-type", "application/json")
        for k, v in (headers or {}).items():
            req.add_header(k, v)
        with urllib_request.urlopen(req, timeout=10) as r:
            return json.loads(r.read().decode("utf-8"))

    def _get_json(self, path: str, params: dict, headers: dict) -> dict:
        url = f"{self._base_url}{path}?{urllib_parse.urlencode(params)}"
        req = urllib_request.Request(url, method="GET")
        for k, v in headers.items():
            req.add_header(k, v)
        try:
            with urllib_request.urlopen(req, timeout=10) as r:
                return json.loads(r.read().decode("utf-8"))
        except urllib_error.HTTPError as e:
            raise RuntimeError(
                f"KIS HTTP {e.code}: {e.read().decode('utf-8', errors='replace')[:200]}"
            ) from e

    def _ensure_token(self) -> None:
        if self._token:
            return
        resp = self._post_json(
            "/oauth2/tokenP",
            {
                "grant_type": "client_credentials",
                "appkey": self._app_key,
                "appsecret": self._app_secret,
            },
        )
        self._token = resp["access_token"]

    def get_stock_info(self, symbol: str) -> dict:
        """KIS search-stock-info (TR_ID: CTPF1604R) — 종목 마스터 조회."""
        self._ensure_token()
        body = self._get_json(
            "/uapi/domestic-stock/v1/quotations/search-stock-info",
            params={"PDNO": symbol, "PRDT_TYPE_CD": "300"},
            headers={
                "authorization": f"Bearer {self._token}",
                "appkey": self._app_key,
                "appsecret": self._app_secret,
                "tr_id": "CTPF1604R",
                "custtype": "P",
            },
        )
        if body.get("rt_cd") != "0":
            raise RuntimeError(f"KIS error symbol={symbol}: {body.get('msg1')}")
        return body.get("output", {})
