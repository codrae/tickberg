"""KIS OAuth — REST access_token + WebSocket approval_key.

토큰을 디스크에 캐시하고 만료 직전까지 재사용한다 (1일 1회 발급 한도 보호).
정시 03:30 KST refresh 는 token_refresher.py 가 담당. ensure_valid() 는
시작 시·필요 시 호출되며 캐시가 유효하면 HTTP 호출 없이 반환.
"""
from __future__ import annotations

import json
import logging
from collections.abc import Callable
from datetime import datetime, timedelta
from pathlib import Path
from typing import Protocol
from zoneinfo import ZoneInfo

log = logging.getLogger("kis_auth")
KST = ZoneInfo("Asia/Seoul")
_SAFETY_BUFFER = timedelta(minutes=5)


class _HttpClient(Protocol):
    async def post(self, path: str, *, json: dict, headers: dict | None = ...) -> dict: ...


class KisAuth:
    def __init__(
        self,
        *,
        app_key: str,
        app_secret: str,
        base_url: str,
        http: _HttpClient,
        cache_path: Path | None = None,
        clock: Callable[[], datetime] = lambda: datetime.now(KST),
    ):
        self._app_key = app_key
        self._app_secret = app_secret
        self._base_url = base_url.rstrip("/")
        self._http = http
        self._cache_path = cache_path
        self._clock = clock
        self._access_token: str | None = None
        self._approval_key: str | None = None
        self._expires_at: datetime | None = None

    @property
    def access_token(self) -> str | None:
        return self._access_token

    @property
    def approval_key(self) -> str | None:
        return self._approval_key

    @property
    def expires_at(self) -> datetime | None:
        return self._expires_at

    async def ensure_valid(self) -> None:
        """캐시(메모리→디스크) 확인 후 만료 시에만 refresh."""
        if self._is_memory_valid():
            return
        if self._load_from_disk():
            return
        await self.refresh()

    async def refresh(self) -> None:
        """REST + WebSocket key 동시 갱신. 실패 시 기존 토큰 유지."""
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
        expires_in = int(token_resp.get("expires_in", 86400))

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
        self._expires_at = self._clock() + timedelta(seconds=expires_in) - _SAFETY_BUFFER
        self._save_to_disk()

    def _is_memory_valid(self) -> bool:
        if not self._access_token or not self._approval_key or not self._expires_at:
            return False
        return self._clock() < self._expires_at

    def _load_from_disk(self) -> bool:
        if not self._cache_path or not self._cache_path.exists():
            return False
        try:
            data = json.loads(self._cache_path.read_text())
            expires_at = datetime.fromisoformat(data["expires_at"])
            if self._clock() >= expires_at:
                return False
            self._access_token = data["access_token"]
            self._approval_key = data["approval_key"]
            self._expires_at = expires_at
            log.info("loaded cached token from %s (expires %s)", self._cache_path, expires_at.isoformat())
            return True
        except (json.JSONDecodeError, KeyError, ValueError) as e:
            log.warning("invalid token cache (%s) — will refetch", e)
            return False

    def _save_to_disk(self) -> None:
        if not self._cache_path or not self._expires_at:
            return
        self._cache_path.parent.mkdir(parents=True, exist_ok=True)
        self._cache_path.write_text(json.dumps({
            "access_token": self._access_token,
            "approval_key": self._approval_key,
            "expires_at": self._expires_at.isoformat(),
        }))
