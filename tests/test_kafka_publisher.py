import json
from datetime import datetime
from decimal import Decimal
from unittest.mock import AsyncMock

import pytest

from src.kafka_publisher import KafkaTickPublisher


@pytest.mark.asyncio
async def test_publish_uses_symbol_as_partition_key_and_serializes_decimals():
    sent = []
    sender = AsyncMock(side_effect=lambda topic, value, key: sent.append((topic, value, key)))
    pub = KafkaTickPublisher(topic="kis.tick.raw", send=sender)

    await pub.publish({
        "symbol": "005930",
        "trade_ts_kst": datetime(2026, 5, 8, 9, 30, 1),
        "price": Decimal("72500"),
        "volume": 100,
        "trade_side": "+",
        "best_ask_price": Decimal("72500"),
        "best_bid_price": Decimal("72400"),
        "cum_volume": 1234567,
        "cum_amount": 89500000000,
        "raw_payload": "005930|093001|72500|...",
    })

    assert sent[0][0] == "kis.tick.raw"
    assert sent[0][2] == b"005930"
    decoded = json.loads(sent[0][1])
    assert decoded["price"] == "72500"
    assert decoded["trade_ts_kst"] == "2026-05-08T09:30:01"
