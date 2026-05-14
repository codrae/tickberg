"""pytest 공통 설정.

driver 타임존을 Asia/Seoul 로 고정 — pyspark `createDataFrame` 가 naive
datetime 을 내부 timestamp 로 변환할 때 `spark.sql.session.timeZone` 이 아니라
**driver process 의 system tz** 를 사용한다. 컨테이너 기본 tz(UTC)면 테스트의
naive datetime(09:30 = KST 의도)이 09:30 UTC 로 해석돼 session tz(Asia/Seoul)
기준 추출 시 18:30 으로 어긋난다. 모든 stage 가 KST 벽시계로 일관되도록 고정.
"""
import os
import time

os.environ["TZ"] = "Asia/Seoul"
time.tzset()
