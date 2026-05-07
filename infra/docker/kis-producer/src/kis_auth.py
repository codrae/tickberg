"""KIS OAuth — REST access_token + WebSocket approval_key.

만료 추적 로직 의도적으로 없음. KisAuth.refresh() 는 매일 03:30 KST 외부
스케줄러 (token_refresher.py) 가 호출. 첫 가동 시 (start-up) 도 1회 호출.
"""
from __future__ import annotations

from typing import Protocol


class _HttpClient(Protocol):
    async def post(self, path: str, *, json: dict, headers: dict | None = ...) -> dict: ...


class KisAuth:
    def __init__(self, *, app_key: str, app_secret: str, base_url: str, http: _HttpClient):
        self._app_key = app_key
        self._app_secret = app_secret
        self._base_url = base_url.rstrip("/")
        self._http = http
        self._access_token: str | None = None
        self._approval_key: str | None = None

    @property
    def access_token(self) -> str | None:
        return self._access_token

    @property
    def approval_key(self) -> str | None:
        return self._approval_key

    async def refresh(self) -> None:
        """Refresh access_token and approval_key atomically.

        실패 시 기존 토큰 유지 (caller 가 retry 또는 alert 결정).
        """
        token_resp = await self._http.post(
            "/oauth2/tokenP",
            json={
                "grant_type": "client_credentials",
                "appkey": self._app_key,
                "appsecret": self._app_secret,
            },
            headers={"content-type": "application/json"},
        )
        new_token = token_resp["access_token"]

        approval_resp = await self._http.post(
            "/oauth2/Approval",
            json={
                "grant_type": "client_credentials",
                "appkey": self._app_key,
                "secretkey": self._app_secret,
            },
            headers={"content-type": "application/json"},
        )
        new_approval = approval_resp["approval_key"]

        self._access_token = new_token
        self._approval_key = new_approval
