from datetime import datetime
from zoneinfo import ZoneInfo

from src.token_refresher import next_refresh_at

KST = ZoneInfo("Asia/Seoul")


def test_next_refresh_today_when_before_0330():
    now = datetime(2026, 5, 8, 3, 0, tzinfo=KST)
    assert next_refresh_at(now).isoformat() == "2026-05-08T03:30:00+09:00"


def test_next_refresh_tomorrow_when_after_0330():
    now = datetime(2026, 5, 8, 3, 31, tzinfo=KST)
    assert next_refresh_at(now).isoformat() == "2026-05-09T03:30:00+09:00"


def test_exactly_0330_picks_tomorrow():
    now = datetime(2026, 5, 8, 3, 30, tzinfo=KST)
    assert next_refresh_at(now).isoformat() == "2026-05-09T03:30:00+09:00"
