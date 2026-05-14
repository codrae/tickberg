# 트러블슈팅 — 2026-05-14 Phase 1B 운영 발견 이슈 6건

> 5/14 영업시간(09:00–15:30 KST) 시스템을 발표 녹화-ready 상태로 만드는 과정에서
> 발견·수정한 운영 이슈 모음. 각 항목은 **증상 → 진단 → 원인 → 수정 → 재발방지**
> 구조. 6개월 후 새 팀원이 같은 증상을 만났을 때 진단 경로를 재현하기 위함.

| # | 이슈 | 심각도 | 상태 |
|---|---|---|---|
| 1 | timezone 불일치 — Gold 가 0 rows | 높음 (데이터 정합성) | 수정 완료 `925c468` |
| 2 | Bronze small-files — bronze→silver 20분 소요 | 중간 (성능) | 완화 `925c468`, 근본수정 Phase 2 |
| 3 | DAG cascade 실패 — silver→gold sensor timeout | 높음 (파이프라인 정지) | 수정 완료 `925c468` |
| 4 | Kafka DuplicateBrokerRegistration 재시작 루프 | 높음 (스트리밍 정지) | 수정 완료 (runtime) |
| 5 | aiokafka producer stuck | 높음 (스트리밍 정지) | 완화 `8e98ec2`, 근본수정 Phase 2 |
| 6 | JMX 9999 포트 충돌 — kafka CLI 실패 | 낮음 (운영 편의) | 수정 완료 `91fb7ad` |

---

## 1. timezone 불일치 — Gold 가 0 rows

### 증상
`silver_to_gold_vwap.py` 가 정상 종료하는데 `gold overwrite hour=... rows=0`.
Athena `gold_symbol_vwap_1m` 에 당일 데이터가 안 들어옴. Silver 는 정상 적재 중.

### 진단
```sql
-- Silver 의 trade_ts_kst 시간대 분포
SELECT hour(trade_ts_kst) hr, count(*) FROM tickberg.silver_kis_tick_clean
GROUP BY hour(trade_ts_kst) ORDER BY hr;
-- → 장중(9–15시)이어야 할 데이터가 hour 0–8 에 몰림 = 9시간 shift
```
Spark 에서 직접 조회 시 `trade_ts_kst` dtype = `timestamp_ntz`,
`F.lit(datetime)` 필터는 0 rows, SQL `timestamp '...'` 리터럴은 정상.

### 원인
세 Spark job 의 `spark.sql.session.timeZone` 설정이 불일치:
- streaming job: `Asia/Seoul` ✅
- bronze→silver, gold job: 미설정 = UTC ❌

데이터 흐름:
1. Producer → Kafka: `"trade_ts_kst": "2026-05-14T13:19:36"` (KST 벽시계 문자열)
2. streaming(`Asia/Seoul`): `to_timestamp` → instant `04:19 UTC` → Bronze parquet
3. **bronze→silver(UTC)**: Bronze instant 을 UTC 로 읽어 Silver Iceberg
   `timestamp`(zoneless) 에 `"04:19"` 로 기록 → 9시간 어긋남
4. gold: Silver 의 `"04:19"` 를 hour 필터 → 0 rows

추가로 gold 의 `F.lit(datetime)` 은 `timestamp`(TZ) 리터럴이라 Silver 의
`timestamp_ntz` 컬럼과 타입 불일치 → 비교가 0 rows.

### 수정 (`925c468`)
- bronze→silver, gold job 에 `.config("spark.sql.session.timeZone", "Asia/Seoul")` 추가
- gold 의 hour 필터를 `F.lit(datetime)` → `F.year/month/dayofmonth/hour` 컴포넌트
  추출 비교로 변경 — `timestamp`·`timestamp_ntz` 양쪽 타입에서 동작
- `tests/conftest.py` 에 `os.environ["TZ"]="Asia/Seoul" + time.tzset()` —
  pyspark `createDataFrame(naive_datetime)` 는 `session.timeZone` 이 아니라
  **driver process 의 system tz** 로 naive→instant 변환하므로

### 재발방지
- 모든 신규 Spark job 의 `SparkSession.builder` 에 `session.timeZone=Asia/Seoul`
  명시 (streaming 과 일치)
- `trade_ts_kst` 는 의미상 KST 벽시계 → Iceberg `timestamp`(zoneless) 가 맞음.
  TZ-aware `timestamp` 와 섞지 말 것
- 테스트는 `tests/conftest.py` 가 driver TZ 를 고정 → 일관성 유지

---

## 2. Bronze small-files — bronze→silver 20분 소요

### 증상
`bronze_to_silver_kis` DAG run 이 7,961 rows MERGE 에 ~15–22분 소요
(정상이면 1–2분). 시간이 갈수록 점점 느려짐.

### 진단
```bash
# Bronze 파일 수
aws s3 ls s3://tickberg-lakehouse/bronze/kis_tick_raw/ --recursive | grep -c parquet
# → 4015 (오늘 dt 만 731)
```
task 로그에 `WARN MetricsConfig` 이후 15분간 로그 공백 → 파일 listing/footer
읽기에서 멈춤.

### 원인
`_read_bronze_window` 가 `spark.read.parquet(bronze_path)` 로 Bronze 전체
트리를 읽은 뒤 `ingest_ts` 로 필터. `ingest_ts` 는 파티션 컬럼(`dt`, `hr`)이
아니라 partition pruning 이 안 됨 → Bronze 전체 history(수천 small-files,
streaming 1분 trigger 누적)의 footer 를 매번 읽음 → O(전체 history).

### 수정 (`925c468`)
`_read_bronze_window` 에 `dt` 파티션 컬럼 필터 추가
(`F.col("dt") >= lookback_date`) → Spark 가 오늘 파티션만 listing.

### 재발방지
- **근본 원인은 Bronze small-files 누적** — partition pruning 은 완화일 뿐.
  근본 수정은 Bronze compaction (Parquet 이라 Iceberg compaction 대상 아님 →
  별도 일배치 또는 streaming writer 의 `coalesce`) = **Phase 2**.
- Parquet append-only layer 에서 streaming trigger 간격이 짧으면 small-files
  는 구조적 — `docs/architecture/100x-scale.md` §5 참조.
- 비파티션 컬럼으로 필터하기 전에 항상 파티션 컬럼으로 먼저 좁힐 것.

---

## 3. DAG cascade 실패 — silver→gold sensor timeout

### 증상
`silver_to_gold_vwap` DAG 의 `wait_for_bronze_to_silver` ExternalTaskSensor 가
900s timeout 으로 실패 반복. Gold 가 5/8 이후 갱신 안 됨.

### 진단
```bash
docker exec tickberg-airflow-scheduler airflow dags list-runs -d silver_to_gold_vwap
# → 대부분 failed, 간헐적 success
```
Spark master UI(`localhost:8081`): Worker 4 cores 전부 사용 중 —
`streaming(2) + bronze_to_silver(2)` = 4, `silver_to_gold` 는 `WAITING cores=0`.

### 원인
두 가지가 곱해진 결과:
- **(a) 작업이 느림** — 이슈 #2 의 bronze→silver 20분 소요
- **(b) 자원 경합** — Worker 4 cores, streaming app 이 2 cores 상주.
  batch 가 2 cores 면 `bronze→silver` + `silver→gold` 동시 실행 불가
  (4 = streaming 2 + batch 2, 남는 게 0). silver→gold 가 cores 를 못 받아
  sensor 가 15분 timeout → 실패. 또 bronze→silver(20분)가 `*/10` 스케줄보다
  길어 `max_active_runs=1` 로 run 이 영구 적체.

### 수정 (`925c468`)
- `_common.py`: `--total-executor-cores 2 → 1`
  → `streaming(2) + bronze→silver(1) + silver→gold(1)` = 4, 동시 실행 가능
- `bronze_to_silver_kis.py`·`silver_to_gold_vwap.py`: schedule `*/10 → */30`
  → 20분 job 에 10분 margin
- `silver_to_gold_vwap.py`: sensor timeout `15min → 25min`

### 재발방지
- 로컬 Spark 단일 클러스터에서 long-running streaming + batch 공존 시,
  batch executor cores 는 `worker_cores - streaming_cores` 안에서 동시
  실행 가능하도록 배분
- 스케줄 간격은 job 실제 소요시간보다 길게 (job 소요 ≤ schedule interval)
- 100x 진화 시 EMR Serverless 로 streaming/batch application 분리 →
  `docs/architecture/100x-scale.md` §3 참조

---

## 4. Kafka DuplicateBrokerRegistration 재시작 루프

### 증상
`tickberg-kafka` 가 `Up N seconds (health: starting)` ↔ `unhealthy` 를 무한
반복. producer 는 `[Errno 111] Connect call failed ('172.19.0.x', 9092)`.

### 진단
```bash
docker logs tickberg-kafka 2>&1 | grep -iE "DuplicateBroker|error"
# → INFO [BrokerLifecycleManager id=1] Unable to register broker 1 because
#   the controller returned error DUPLICATE_BROKER_REGISTRATION
```

### 원인
Kafka(KRaft 모드) 컨테이너가 (트리거 미확정 — host 메모리 압박 추정,
당시 Spark executor OOM `code 137` 동시 발생) 재시작됨. `restart:
unless-stopped` 가 빠르게 재시작하는데, KRaft controller 메타데이터에 **이전
broker 인스턴스(id=1)의 등록이 아직 expire 안 됨** → 새 broker 가 같은 id 로
등록 시도 → `DuplicateBrokerRegistrationException` → healthcheck 실패 →
재시작 → 반복. 자력 회복 불가 루프.

### 수정 (runtime, 코드 변경 없음)
```bash
docker compose -f infra/docker/docker-compose.yml stop kafka
sleep 20   # controller 가 이전 broker session 을 expire 시킬 gap 확보
docker compose -f infra/docker/docker-compose.yml start kafka
```
`restart` 가 아니라 명시적 `stop → 대기 → start` — 깨끗한 종료 gap 이 핵심.
4번째 health 체크에서 `healthy` 복구.

### 재발방지
- healthcheck 에 `start_period: 30s` 추가 (`8e98ec2`) — 부팅 grace window
- KRaft 단일 노드 재시작 시 `restart` 대신 `stop → 대기 → start` 사용
- 근본은 **최초 크래시 트리거(host 메모리)** 제거 — 동시 실행 Spark job 수
  제한 (이슈 #3 의 cores 조정이 부분 기여). 100x 에선 MSK Serverless 로
  단일 broker SPOF 자체를 제거.

---

## 5. aiokafka producer stuck

### 증상
Kafka 가 정상 복구된 뒤에도 `tickberg-kis-producer` 가
`[Errno 111] Connect call failed` + `NodeNotReadyError` 를 100ms 주기로
무한 반복. KIS WebSocket 수신은 정상인데 Kafka produce 만 안 됨.

### 진단
**결정적 모순 — raw socket vs aiokafka:**
```bash
# raw socket 은 같은 주소에 정상 접속
docker exec tickberg-kis-producer python -c \
  "import socket; print(socket.create_connection(('kafka', 9092), 3))"
# → <socket.socket ... raddr=('172.19.0.x', 9092)>  ✅

# aiokafka 만 같은 주소로 실패 ❌
```
같은 컨테이너·같은 주소·같은 포트에 raw socket 은 붙는데 aiokafka 만 실패
→ 네트워크/broker/설정 문제가 아니라 **클라이언트 내부 상태 문제** 확정.

타임스탬프 정렬: producer 에러 시각과 broker `Transition to STARTED` 시각이
~400ms 차이 → broker 가 listener 막 여는 순간 producer 가 접속 시도 중이었음.

### 원인
broker 재시작 직후 startup race 윈도우에서 aiokafka 의 노드별 connection
상태가 `disconnected` 도 `connected` 도 아닌 stuck 상태로 빠짐. 이후 broker 가
완전 정상이 되어도 stale state 를 들고 이전 시도를 계속 retry — 새 connection
을 안 만듦. (aiokafka 이슈 트래커에 유사 케이스 다수.)

### 수정 (runtime)
```bash
docker restart tickberg-kis-producer
```
client 인스턴스가 새로 생성되며 모든 내부 state 초기화 → 즉시 정상화.

### 재발방지
- **부분 완화 (`8e98ec2`)**:
  - `AIOKafkaProducer` 에 `metadata_max_age_ms=30000` (기본 5분→30초),
    `request_timeout_ms`·`retry_backoff_ms` — stale 윈도우 단축
  - kafka healthcheck `start_period` (이슈 #4 와 공유)
- **근본 방지 = Phase 2**: producer 의 `restart: unless-stopped` 는 이 케이스를
  못 막음 — 프로세스가 *죽지 않고* stuck-loop 만 돌기 때문. 유일한 근본 방지는
  **self-healing guard** — 연속 send 실패가 임계치를 넘으면 client 인스턴스를
  재생성하는 래퍼(`ResilientProducer`). aiokafka 내부 reconnect 를 믿지 말고
  한 단계 위에서 "안 되면 갈아엎는다". stateful 클라이언트(confluent-kafka,
  redis-py, asyncpg 등) 전반에 일반화 가능한 패턴.

---

## 6. JMX 9999 포트 충돌 — kafka CLI 실패

### 증상
`docker exec tickberg-kafka kafka-topics.sh ...` 또는 `kafka-console-consumer.sh`
실행 시:
```
java.rmi.server.ExportException: Port already in use: 9999
java.net.BindException: Address already in use
```

### 진단
```bash
docker exec tickberg-kafka env | grep -i jmx
# → KAFKA_JMX_OPTS=...-Dcom.sun.management.jmxremote.port=9999...
```
broker JVM 이 이미 9999 를 점유 중 (`tickberg-kafka-jmx` exporter 가 붙음).

### 원인
bitnami kafka 이미지가 컨테이너 전역 환경변수로 `KAFKA_JMX_OPTS`(port 9999
포함)를 설정. `docker exec` 로 같은 컨테이너 안에서 새 JVM(`kafka-topics.sh`
등)을 띄우면 이 환경변수를 그대로 상속 → 새 JVM 도 9999 bind 시도 → 충돌.

### 수정 (`91fb7ad`)
명령 실행 시 `KAFKA_JMX_OPTS` 만 비우면 됨 (이 이미지는 별도 `JMX_PORT` env
없음 — JMX 설정이 전부 `KAFKA_JMX_OPTS` 안):
```bash
docker exec -e KAFKA_JMX_OPTS="" tickberg-kafka kafka-topics.sh \
  --bootstrap-server localhost:9092 --list
```
`smoke_check.sh` 의 `[2]` 단계가 이 패턴 적용. kafka healthcheck 도 이미
`KAFKA_JMX_OPTS= kafka-topics.sh ...` 로 면역.

### 재발방지
- kafka 컨테이너 내부에서 CLI 실행 시 항상 `-e KAFKA_JMX_OPTS=""` (또는
  컨테이너 안에서 `unset KAFKA_JMX_OPTS`) 프리픽스
- 토픽/메시지 확인은 `kafka-ui`(`localhost:8090`) 사용이 가장 편함 — CLI 불필요

---

## 회고 — 디버깅 방법론

이번 6건에서 진단을 빠르게 좁힌 결정적 기법 2가지 (분산 시스템 전반에 유효):

1. **한 layer 아래에서 똑같이 시도하기** — 이슈 #5 에서 raw socket 으로 같은
   주소에 붙어봄 → 성공 → "네트워크는 정상" 확정 → 의심 범위가 클라이언트로
   좁혀짐. 이슈 #1 에서 Spark 로 직접 조회 → `F.lit` vs SQL 리터럴 차이 발견.
2. **이벤트 시간 정렬** — 이슈 #5 에서 producer 에러 시각과 broker STARTED
   시각을 같은 기준(UTC)으로 맞춰 race condition 가설 확정. 이슈 #1 에서
   `silver_ts` vs `trade_ts_kst` 비교로 9시간 shift 발견.

공통 교훈: **설정 일관성**(이슈 #1·#6 — session.timeZone, KAFKA_JMX_OPTS) 과
**자원 한계 인식**(이슈 #2·#3·#4 — Bronze small-files, 4 cores, host 메모리)이
Phase 1 로컬 Docker 환경의 주요 함정. 100x 진화 시 대부분 관리형 서비스
(EMR Serverless, MSK)로 흡수됨 — `docs/architecture/100x-scale.md` 참조.

---

## 부록 — 자주 쓰는 진단 커맨드

```bash
# 전체 헬스체크 (8단계)
bash infra/scripts/smoke_check.sh

# Spark 클러스터 자원 현황
curl -s localhost:8081/json/ | python3 -c \
  "import json,sys; d=json.load(sys.stdin); print('cores',d['coresused'],'/',d['cores']); \
   [print(' ',a['name'],a['state'],a.get('cores')) for a in d['activeapps']]"

# Kafka 상태 / 토픽 (JMX 충돌 회피)
docker exec -e KAFKA_JMX_OPTS="" tickberg-kafka \
  kafka-topics.sh --bootstrap-server localhost:9092 --list
docker logs tickberg-kafka 2>&1 | grep -iE "DuplicateBroker|error|STARTED"

# producer 상태
curl -s localhost:9100/metrics | grep '^kis_'
docker logs --since 2m tickberg-kis-producer 2>&1 | grep -iE "error|errno"

# raw socket 접속 테스트 (네트워크 vs 클라이언트 구분)
docker exec tickberg-kis-producer python -c \
  "import socket; print(socket.create_connection(('kafka', 9092), 3))"

# Airflow DAG run 상태
docker exec tickberg-airflow-scheduler airflow dags list-runs -d <dag_id>

# Silver timezone 검증
# (Athena) SELECT hour(trade_ts_kst), count(*) FROM tickberg.silver_kis_tick_clean GROUP BY 1;
```
