"""KIS producer entrypoint.

장 시간대 (영업일 09:00–15:30 KST) 만 메시지 흐름 발생.
장 마감 후 메시지 0 = 정상 (Grafana ws_connected 만 alert 대상).
"""
from __future__ import annotations

import asyncio
import logging
import os
from datetime import date, datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import aiohttp
import websockets
from aiokafka import AIOKafkaProducer

from src import health_metrics, token_refresher
from src.kafka_publisher import KafkaTickPublisher
from src.kis_auth import KisAuth
from src.kis_websocket import KisWebSocket

KST = ZoneInfo("Asia/Seoul")
log = logging.getLogger("kis_producer")


class _AiohttpKisHttp:
    def __init__(self, base_url: str, session: aiohttp.ClientSession):
        self._base_url = base_url.rstrip("/")
        self._session = session

    async def post(self, path: str, *, json: dict, headers: dict | None = None) -> dict:
        async with self._session.post(self._base_url + path, json=json, headers=headers) as r:
            r.raise_for_status()
            return await r.json()


def _today_kst() -> date:
    return datetime.now(KST).date()


async def _ws_connect(url: str):
    # KIS WebSocket 은 WS-level ping/pong (RFC 6455 control frame) 미지원.
    # 대신 application-level PINGPONG JSON 메시지를 KIS 가 보냄 → 우리는
    # kis_websocket._connect_and_consume() 에서 받아서 ws.pong() 으로 회신.
    # ping_interval=None 으로 우리 측 자동 WS-ping 비활성화 (이전엔 30초
    # ping → 20초 pong 미수신 → 1011 keepalive timeout disconnect 반복).
    return await websockets.connect(url, ping_interval=None, ping_timeout=None)


async def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")

    app_key = os.environ["KIS_APP_KEY"]
    app_secret = os.environ["KIS_APP_SECRET"]
    base_url = os.environ.get("KIS_BASE_URL", "https://openapi.koreainvestment.com:9443")
    ws_url = os.environ.get("KIS_WS_URL", "ws://ops.koreainvestment.com:21000")
    symbols = [s.strip() for s in os.environ["KIS_SYMBOLS"].split(",") if s.strip()]
    bootstrap = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")
    topic = os.environ.get("KAFKA_TOPIC_TICK", "kis.tick.raw")

    health_metrics.start(port=int(os.environ.get("METRICS_PORT", "9100")))

    cache_path = Path(os.environ.get("KIS_TOKEN_CACHE", "/app/data/kis_token.json"))

    async with aiohttp.ClientSession() as session:
        auth = KisAuth(
            app_key=app_key, app_secret=app_secret, base_url=base_url,
            http=_AiohttpKisHttp(base_url, session),
            cache_path=cache_path,
        )
        await auth.ensure_valid()

        producer = AIOKafkaProducer(
            bootstrap_servers=bootstrap,
            acks="all",
            enable_idempotence=True,
            linger_ms=50,
            compression_type="snappy",
            # broker 재시작 후 stale metadata 윈도우 단축 (기본 5분 → 30초).
            # stuck 케이스 자체의 근본 방지는 아님 (aiokafka 내부 reconnect 한계) —
            # self-healing guard (연속 실패 시 client 재생성) 는 Phase 2.
            request_timeout_ms=30000,
            retry_backoff_ms=500,
            metadata_max_age_ms=30000,
        )
        await producer.start()
        try:
            publisher = KafkaTickPublisher(
                topic=topic,
                send=lambda t, v, k: producer.send_and_wait(t, value=v, key=k),
            )

            ws = KisWebSocket(
                ws_url=ws_url, auth=auth, symbols=symbols,
                ws_connect=_ws_connect,
                on_connect=lambda b: health_metrics.ws_connected.set(1 if b else 0),
                business_day_provider=_today_kst,
            )

            refresher = asyncio.create_task(
                token_refresher.run(
                    auth,
                    on_failure=lambda _: health_metrics.token_refresh_failures.inc(),
                )
            )

            async for row in ws.stream():
                try:
                    await publisher.publish(row)
                    health_metrics.messages_published.labels(symbol=row["symbol"]).inc()
                except Exception as e:  # noqa: BLE001
                    log.error("publish failed: %s", e)
        finally:
            refresher.cancel()
            await producer.stop()


if __name__ == "__main__":
    asyncio.run(main())
