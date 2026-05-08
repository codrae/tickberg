import pytest

from src.kis_websocket import _next_backoff_seconds


@pytest.mark.parametrize(
    "attempt, expected",
    [(0, 1), (1, 2), (2, 4), (3, 8), (4, 16), (5, 32), (6, 60), (10, 60)],
)
def test_backoff_caps_at_60(attempt, expected):
    assert _next_backoff_seconds(attempt) == expected
