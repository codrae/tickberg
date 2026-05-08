import json
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import AsyncMock
from zoneinfo import ZoneInfo

import pytest

from src.kis_auth import KisAuth

KST = ZoneInfo("Asia/Seoul")


def _http_returning(token: str = "ACC", approval: str = "APP", expires_in: int = 86400):
    rest = AsyncMock()
    rest.post = AsyncMock(side_effect=[
        {"access_token": token, "expires_in": expires_in, "token_type": "Bearer"},
        {"approval_key": approval},
    ])
    return rest


@pytest.mark.asyncio
async def test_token_refresh_calls_oauth_then_websocket_key():
    rest_mock = _http_returning(token="ACC", approval="APP")
    auth = KisAuth(app_key="K", app_secret="S", base_url="https://kis", http=rest_mock)

    await auth.refresh()

    assert auth.access_token == "ACC"
    assert auth.approval_key == "APP"
    assert rest_mock.post.await_count == 2


@pytest.mark.asyncio
async def test_refresh_failure_keeps_previous_token():
    rest_mock = AsyncMock()
    auth = KisAuth(app_key="K", app_secret="S", base_url="https://kis", http=rest_mock)
    auth._access_token = "OLD"  # type: ignore[attr-defined]
    auth._approval_key = "OLDKEY"  # type: ignore[attr-defined]
    rest_mock.post = AsyncMock(side_effect=RuntimeError("network"))

    with pytest.raises(RuntimeError):
        await auth.refresh()

    assert auth.access_token == "OLD"
    assert auth.approval_key == "OLDKEY"


@pytest.mark.asyncio
async def test_refresh_records_expiry_with_safety_buffer():
    rest = _http_returning(expires_in=86400)
    now = datetime(2026, 5, 8, 9, 0, tzinfo=KST)
    auth = KisAuth(
        app_key="K", app_secret="S", base_url="https://kis", http=rest,
        clock=lambda: now,
    )

    await auth.refresh()

    assert auth.expires_at == now + timedelta(seconds=86400) - timedelta(minutes=5)


@pytest.mark.asyncio
async def test_ensure_valid_skips_http_when_token_not_expired():
    rest = _http_returning()
    now = datetime(2026, 5, 8, 9, 0, tzinfo=KST)
    auth = KisAuth(
        app_key="K", app_secret="S", base_url="https://kis", http=rest,
        clock=lambda: now,
    )
    await auth.refresh()
    rest.post.reset_mock()

    await auth.ensure_valid()

    assert rest.post.await_count == 0


@pytest.mark.asyncio
async def test_ensure_valid_refetches_when_in_memory_token_expired():
    rest = AsyncMock()
    rest.post = AsyncMock(side_effect=[
        {"access_token": "T1", "expires_in": 60, "token_type": "Bearer"},
        {"approval_key": "A1"},
        {"access_token": "T2", "expires_in": 60, "token_type": "Bearer"},
        {"approval_key": "A2"},
    ])
    clock_values = iter([
        datetime(2026, 5, 8, 9, 0, tzinfo=KST),
        datetime(2026, 5, 8, 9, 0, tzinfo=KST),
        datetime(2026, 5, 8, 10, 0, tzinfo=KST),
        datetime(2026, 5, 8, 10, 0, tzinfo=KST),
    ])
    auth = KisAuth(
        app_key="K", app_secret="S", base_url="https://kis", http=rest,
        clock=lambda: next(clock_values),
    )
    await auth.refresh()
    assert auth.access_token == "T1"

    await auth.ensure_valid()

    assert auth.access_token == "T2"


@pytest.mark.asyncio
async def test_refresh_persists_cache_to_disk(tmp_path: Path):
    cache = tmp_path / "kis.json"
    rest = _http_returning(token="T", approval="A", expires_in=3600)
    now = datetime(2026, 5, 8, 9, 0, tzinfo=KST)
    auth = KisAuth(
        app_key="K", app_secret="S", base_url="https://kis", http=rest,
        cache_path=cache, clock=lambda: now,
    )

    await auth.refresh()

    saved = json.loads(cache.read_text())
    assert saved["access_token"] == "T"
    assert saved["approval_key"] == "A"
    assert datetime.fromisoformat(saved["expires_at"]) == auth.expires_at


@pytest.mark.asyncio
async def test_ensure_valid_loads_cache_from_disk_when_valid(tmp_path: Path):
    cache = tmp_path / "kis.json"
    expiry = datetime(2026, 5, 8, 23, 0, tzinfo=KST)
    cache.write_text(json.dumps({
        "access_token": "DISK_TOK",
        "approval_key": "DISK_APP",
        "expires_at": expiry.isoformat(),
    }))
    rest = AsyncMock()
    rest.post = AsyncMock()
    now = datetime(2026, 5, 8, 9, 0, tzinfo=KST)
    auth = KisAuth(
        app_key="K", app_secret="S", base_url="https://kis", http=rest,
        cache_path=cache, clock=lambda: now,
    )

    await auth.ensure_valid()

    assert auth.access_token == "DISK_TOK"
    assert auth.approval_key == "DISK_APP"
    assert rest.post.await_count == 0


@pytest.mark.asyncio
async def test_ensure_valid_refetches_when_disk_cache_expired(tmp_path: Path):
    cache = tmp_path / "kis.json"
    cache.write_text(json.dumps({
        "access_token": "STALE",
        "approval_key": "STALE_APP",
        "expires_at": "2026-05-07T23:00:00+09:00",
    }))
    rest = _http_returning(token="FRESH", approval="FRESH_APP")
    now = datetime(2026, 5, 8, 9, 0, tzinfo=KST)
    auth = KisAuth(
        app_key="K", app_secret="S", base_url="https://kis", http=rest,
        cache_path=cache, clock=lambda: now,
    )

    await auth.ensure_valid()

    assert auth.access_token == "FRESH"


@pytest.mark.asyncio
async def test_ensure_valid_refetches_when_disk_cache_corrupt(tmp_path: Path):
    cache = tmp_path / "kis.json"
    cache.write_text("not json {")
    rest = _http_returning(token="FRESH", approval="FRESH_APP")
    now = datetime(2026, 5, 8, 9, 0, tzinfo=KST)
    auth = KisAuth(
        app_key="K", app_secret="S", base_url="https://kis", http=rest,
        cache_path=cache, clock=lambda: now,
    )

    await auth.ensure_valid()

    assert auth.access_token == "FRESH"
