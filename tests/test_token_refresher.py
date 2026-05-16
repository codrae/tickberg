from datetime import datetime
from zoneinfo import ZoneInfo

from src.token_refresher import next_refresh_at

KST = ZoneInfo("Asia/Seoul")

# 2026-05-04 Mon · 05-07 Thu · 05-08 Fri · 05-09 Sat · 05-10 Sun · 05-11 Mon


def test_next_refresh_today_when_before_0330_on_weekday():
    now = datetime(2026, 5, 7, 3, 0, tzinfo=KST)  # Thu
    assert next_refresh_at(now).isoformat() == "2026-05-07T03:30:00+09:00"


def test_next_refresh_tomorrow_when_after_0330_on_weekday():
    now = datetime(2026, 5, 7, 3, 31, tzinfo=KST)  # Thu 03:31 → Fri 03:30
    assert next_refresh_at(now).isoformat() == "2026-05-08T03:30:00+09:00"


def test_exactly_0330_picks_tomorrow_on_weekday():
    now = datetime(2026, 5, 7, 3, 30, tzinfo=KST)  # Thu 03:30 exact → Fri
    assert next_refresh_at(now).isoformat() == "2026-05-08T03:30:00+09:00"


def test_friday_after_0330_skips_to_monday():
    """Fri 03:31 → Sat·Sun skip → Mon 03:30."""
    now = datetime(2026, 5, 8, 3, 31, tzinfo=KST)  # Fri
    assert next_refresh_at(now).isoformat() == "2026-05-11T03:30:00+09:00"


def test_saturday_picks_monday():
    now = datetime(2026, 5, 9, 3, 0, tzinfo=KST)  # Sat
    assert next_refresh_at(now).isoformat() == "2026-05-11T03:30:00+09:00"


def test_sunday_picks_monday():
    now = datetime(2026, 5, 10, 12, 0, tzinfo=KST)  # Sun afternoon
    assert next_refresh_at(now).isoformat() == "2026-05-11T03:30:00+09:00"


def test_sunday_before_0330_still_picks_monday():
    """Sun 03:00 < 03:30 — 평일 분리 전엔 today_target=Sun 03:30 반환하던 케이스."""
    now = datetime(2026, 5, 10, 3, 0, tzinfo=KST)  # Sun 03:00
    assert next_refresh_at(now).isoformat() == "2026-05-11T03:30:00+09:00"
