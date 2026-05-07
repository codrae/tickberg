from unittest.mock import AsyncMock

import pytest

from src.kis_auth import KisAuth


@pytest.mark.asyncio
async def test_token_refresh_calls_oauth_then_websocket_key():
    rest_mock = AsyncMock()
    rest_mock.post = AsyncMock(side_effect=[
        {"access_token": "ACC", "expires_in": 86400, "token_type": "Bearer"},
        {"approval_key": "APP"},
    ])
    auth = KisAuth(
        app_key="K", app_secret="S",
        base_url="https://kis", http=rest_mock,
    )

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
