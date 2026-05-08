"""Kafka publisher — symbol partition key + JSON serialize.

aiokafka 의 producer config (acks=all, enable_idempotence=True, retries=10,
linger_ms=50, compression_type=snappy) 는 main.py 에서 주입.
"""
from __future__ import annotations

import json
from collections.abc import Awaitable, Callable
from datetime import datetime
from decimal import Decimal
from typing import Any

_Sender = Callable[..., Awaitable[Any]]  # send(topic, value, key)


def _default(v: Any) -> Any:
    if isinstance(v, Decimal):
        return str(v)
    if isinstance(v, datetime):
        return v.isoformat(timespec="seconds")
    raise TypeError(f"unserialized type: {type(v)}")


class KafkaTickPublisher:
    def __init__(self, *, topic: str, send: _Sender):
        self._topic = topic
        self._send = send

    async def publish(self, row: dict[str, Any]) -> None:
        symbol = row["symbol"]
        if not isinstance(symbol, str) or not symbol:
            raise ValueError("symbol required")
        payload = json.dumps(row, default=_default, ensure_ascii=False).encode("utf-8")
        await self._send(self._topic, payload, symbol.encode("utf-8"))
