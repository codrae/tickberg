"""KIS WebSocket subscription with reconnect.

영업시간 외 (장 마감 후) 메시지 0 도 정상이라 ws_connected 만 가지고 alert 하지 않음.
호출자 (main.py) 가 영업시간 컨텍스트와 결합해서 판단."""
from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import AsyncIterator, Awaitable, Callable
from datetime import date

from src.kis_auth import KisAuth
from src.parser import ParseError, parse_h0stcnt0

log = logging.getLogger("kis_websocket")

_HEARTBEAT_TIMEOUT_S = 60
_BACKOFF_CAP_S = 60


def _next_backoff_seconds(attempt: int) -> int:
    """1, 2, 4, 8, 16, 32, 60, 60, ... (cap = 60s)."""
    return min(2**attempt, _BACKOFF_CAP_S)


def _build_subscribe_frame(approval_key: str, symbol: str) -> str:
    return json.dumps({
        "header": {
            "approval_key": approval_key,
            "custtype": "P",
            "tr_type": "1",
            "content-type": "utf-8",
        },
        "body": {"input": {"tr_id": "H0STCNT0", "tr_key": symbol}},
    })


class KisWebSocket:
    def __init__(
        self, *,
        ws_url: str,
        auth: KisAuth,
        symbols: list[str],
        ws_connect: Callable[[str], Awaitable["_WsLike"]],
        on_connect: Callable[[bool], None] | None = None,
        business_day_provider: Callable[[], date] = lambda: date.today(),
    ):
        self._ws_url = ws_url
        self._auth = auth
        self._symbols = list(symbols)
        self._ws_connect = ws_connect
        self._on_connect = on_connect or (lambda _b: None)
        self._business_day = business_day_provider

    async def stream(self) -> AsyncIterator[dict]:
        attempt = 0
        while True:
            try:
                async for parsed in self._connect_and_consume():
                    attempt = 0
                    yield parsed
            except asyncio.CancelledError:
                raise
            except Exception as e:  # noqa: BLE001
                self._on_connect(False)
                wait = _next_backoff_seconds(attempt)
                log.warning("ws disconnected (%s). reconnect in %ds", e, wait)
                await asyncio.sleep(wait)
                attempt += 1

    async def _connect_and_consume(self) -> AsyncIterator[dict]:
        if not self._auth.approval_key:
            raise RuntimeError("approval_key missing — call KisAuth.refresh() first")
        ws = await self._ws_connect(self._ws_url)
        try:
            self._on_connect(True)
            for sym in self._symbols:
                await ws.send(_build_subscribe_frame(self._auth.approval_key, sym))
            log.info("subscribed %d symbols", len(self._symbols))

            while True:
                msg = await asyncio.wait_for(ws.recv(), timeout=_HEARTBEAT_TIMEOUT_S)
                if isinstance(msg, str) and msg.startswith("{"):
                    # control frame (subscribe ack / heartbeat). skip.
                    continue
                payload = msg if isinstance(msg, str) else msg.decode("utf-8")
                try:
                    rows = parse_h0stcnt0(payload, business_day=self._business_day())
                except ParseError as e:
                    log.error("parse error: %s | raw=%s", e, payload[:200])
                    continue
                for r in rows:
                    yield r
        finally:
            await ws.close()


class _WsLike:  # pragma: no cover — structural only
    async def send(self, data: str) -> None: ...
    async def recv(self) -> str | bytes: ...
    async def close(self) -> None: ...
