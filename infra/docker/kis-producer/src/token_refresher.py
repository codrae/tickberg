"""Daily 03:30 KST token refresh.

만료시간 추적 안 함 (spec §6.1.1, D15). 매일 03:30 강제 교체.
04:30 cutoff 이후에도 실패 시 critical alert (caller 가 관리).
"""
from __future__ import annotations

import asyncio
import logging
from datetime import datetime, time, timedelta
from zoneinfo import ZoneInfo

from src.kis_auth import KisAuth

log = logging.getLogger("token_refresher")
KST = ZoneInfo("Asia/Seoul")
_REFRESH_TIME = time(3, 30)
_RETRY_BACKOFF_S = 5 * 60   # 5분 간격
_CUTOFF_TIME = time(4, 30)


def next_refresh_at(now: datetime) -> datetime:
    today_target = now.replace(
        hour=_REFRESH_TIME.hour, minute=_REFRESH_TIME.minute,
        second=0, microsecond=0,
    )
    if now < today_target:
        return today_target
    return today_target + timedelta(days=1)


async def run(auth: KisAuth, on_failure: callable | None = None) -> None:
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
