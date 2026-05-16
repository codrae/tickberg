"""Weekday 03:30 KST token refresh.

만료시간 추적 안 함 (spec §6.1.1, D15). MON–FRI 03:30 강제 교체.
주말(토·일)엔 KIS 정규장이 없어 토큰 사용 0건 → refresh skip
(불필요한 KIS POST 차단). 04:30 cutoff 이후 실패 시 critical alert.
"""
from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable
from datetime import datetime, time, timedelta
from zoneinfo import ZoneInfo

from src.kis_auth import KisAuth

log = logging.getLogger("token_refresher")
KST = ZoneInfo("Asia/Seoul")
_REFRESH_TIME = time(3, 30)
_RETRY_BACKOFF_S = 70   # KIS 토큰 발급 1분 1회 throttle 회피용 70초
_CUTOFF_TIME = time(4, 30)


def next_refresh_at(now: datetime) -> datetime:
    """다음 평일 03:30 KST. weekday=0(Mon)..4(Fri), 5/6=Sat/Sun."""
    today_target = now.replace(
        hour=_REFRESH_TIME.hour, minute=_REFRESH_TIME.minute,
        second=0, microsecond=0,
    )
    target = today_target if now < today_target else today_target + timedelta(days=1)
    while target.weekday() >= 5:
        target += timedelta(days=1)
    return target


async def run(
    auth: KisAuth, on_failure: Callable[[Exception], None] | None = None
) -> None:
    while True:
        now = datetime.now(KST)
        target = next_refresh_at(now)
        sleep_s = (target - now).total_seconds()
        log.info("next refresh at %s (sleep %.0fs)", target.isoformat(), sleep_s)
        await asyncio.sleep(sleep_s)

        success = False
        while not success:
            try:
                await auth.refresh()
                success = True
                log.info("token refreshed")
            except Exception as e:  # noqa: BLE001
                if on_failure:
                    on_failure(e)
                if datetime.now(KST).time() > _CUTOFF_TIME:
                    log.critical("refresh failed past 04:30 cutoff: %s", e)
                    break
                log.warning("refresh failed, retry in %ds: %s", _RETRY_BACKOFF_S, e)
                await asyncio.sleep(_RETRY_BACKOFF_S)
