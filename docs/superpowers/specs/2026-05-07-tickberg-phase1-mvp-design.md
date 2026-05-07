# tickberg Phase 1 MVP — Design

| | |
|---|---|
| Author | codrae |
| Date | 2026-05-07 (목) |
| Status | Draft (브레인스톰 승인 후 작성) |
| 1차 발표 | 2026-05-10 (일) 저녁 8시 |
| 최종 발표 | 2026-05-16 (토) |
| Skill | superpowers:brainstorming → superpowers:writing-plans |

---

## 1. 개요

### 1.1 미션

한국투자증권(KIS) Open API 실시간 체결가를 Kafka·Spark Streaming·Iceberg·Athena·QuickSight 한 줄기로 통과시켜 **5/10 1차 발표에 동작하는 end-to-end 데모**를 보이고, 5/11–5/15 영업일 5일 동안 DART/신용정보원 통합·운영 가시성 강화·100x scale 설계 문서를 추가하여 **5/16 최종 발표**에 부트캠프 평가 4가지 (운영 가시성·100x scale 사고력·Iceberg 필요성·협업·지속가능성) 를 모두 만족시킨다.

### 1.2 범위 — 1차 vs 최종

| 항목 | 5/10 1차 | 5/16 최종 |
|---|---|---|
| 데이터 소스 | 한투 실시간만 (E2E) | + DART 공시 (Bronze + Silver) + 신용정보원 (Bronze) |
| Iceberg MERGE | dim_symbol 액면분할 시연 | 통합 검증 + edge case 단위 테스트 |
| Iceberg 매니지먼트 자동화 | Compaction Airflow DAG 1개 | + expire_snapshots 주간 DAG |
| 운영 가시성 | Grafana 2 패널 + QuickSight 운영탭 3 viz + Health query 4개 | + Grafana 4 패널 / + QuickSight 3 viz / + Health query 3개 |
| 100x scale 사고 | 발표 메시지 일부 | dimension 4개 분해 + 비용 worst-case + evolution 경로 design doc |
| 통합 테스트 | KIS parser unit + MERGE 통합 (demo prep 겸용) | + Spark Streaming 통합 + E2E replay 검증 |

### 1.3 제약·자원

| 항목 | 값 |
|---|---|
| 1차 발표일 | 2026-05-10 (일) **= 비영업일** |
| 최종 발표일 | 2026-05-16 (토) **= 비영업일** |
| 5/7–5/10 가용 시간 | 8h/일 × 3일 ≈ 24h |
| 5/11–5/15 가용 시간 | 8h/일 × 5일 = 40h (영업일, 시스템 자동 적재 위에서 작업) |
| KIS API | 실계좌 키 보유 + WebSocket 검증 완료 |
| AWS | 계정 + Iceberg 핸즈온 OK. tickberg 전용 리소스 신규 |
| 종목 universe | **3 종목 — 삼성전자(005930), SK하이닉스(000660), NAVER(035420)** |
| 로컬 Docker 스택 | 부트캠프 compose 재사용 가능 |
| user 강점 | Airflow · SQL · ETL 설계 (KTX ETL 실무) |
| user 우려 | Kafka · Spark Streaming · Iceberg 첫 사용 / KIS WebSocket 안정성 / AWS 비용 |

### 1.4 평가 기준 매핑

| 평가 기준 | 본 디자인 응답 위치 |
|---|---|
| 운영 가시성 (5분 헬스체크) | §5 (4-tier + 시나리오) |
| 100x 스케일 사고력 | §8 (dimension 4 + worst-case 비용 + evolution) |
| Iceberg 필요성 | §3.5 (3 가치 ↔ 3 위치 매핑) |
| 협업·지속가능성 | CLAUDE.md + 본 design doc + writing-plans 산출물 |

---

## 2. System Topology

### 2.1 컴포넌트 (CLAUDE.md "AWS 단일, 컴퓨트만 로컬" 결정 구체화)

| # | 컴포넌트 | 책임 | 구현 | 위치 |
|---|---|---|---|---|
| 1 | KIS Producer (Python) | KIS OAuth + WebSocket 구독 + Kafka publish | aiohttp + aiokafka, asyncio | docker-compose 별도 서비스 (`restart=unless-stopped`) |
| 2 | Spark Streaming (Bronze) | Kafka → Bronze Parquet (1분 micro-batch) | Structured Streaming, file sink | Spark cluster (long-running, Airflow 외부) |
| 3 | Spark Batch — Bronze→Silver | dedup MERGE INTO | PySpark + Iceberg | Airflow `SparkSubmitOperator` |
| 4 | Spark Batch — Silver→Gold | hour partition OVERWRITE | PySpark + Iceberg | Airflow `SparkSubmitOperator` |
| 5 | Spark Batch — Compaction · dim_symbol | RewriteDataFiles + 종목마스터 일배치 | PySpark + Iceberg Action | Airflow `SparkSubmitOperator` |
| 6 | Airflow | DAG 5개 통괄 | LocalExecutor + PostgreSQL | docker-compose |
| 7 | Kafka (KRaft) | 메시지 큐 (단일 broker, RF=1). Topic·partition 설계는 §2.4 참조 | OSS | docker-compose |
| 8 | Prometheus + Grafana | 1차 메트릭 2 패널 | Kafka JMX exporter, Spark Prometheus servlet | docker-compose |

**AWS 리소스** (`ap-northeast-2`):
- S3 `tickberg-lakehouse` — `bronze/`, `silver/`, `gold/`, `checkpoints/`, `athena-results/`
- Glue Data Catalog DB `tickberg` (기존 학습용 DB와 분리)
- Athena workgroup `tickberg-wg` (`BytesScannedCutoffPerQuery=5GB`)
- QuickSight (Athena 데이터셋 1 + KPI탭 + 운영탭)
- IAM 사용자 `tickberg-user` (S3 단일 버킷 + Glue DB + Athena workgroup 권한 한정)
- AWS Budgets 월 $20 알람

### 2.2 데이터 흐름

```
KIS WS ──▶ KIS Producer ──▶ Kafka `kis.tick.raw`
                                  │
                                  ▼ (Spark Structured Streaming, append, processingTime=1min)
                            S3 Bronze Parquet (dt=YYYY-MM-DD/hr=HH)
                                  │
                                  ▼ (Airflow */5min, 영업시간만 → Spark batch)
                          Silver Iceberg `kis_tick_clean`  (MERGE INTO, dedup by trade_uid)
                          Silver Iceberg `dim_symbol`      (일배치 04:00 MERGE, KIS REST)
                                  │
                                  ▼ (Airflow */5min, 영업시간만 → Spark batch)
                          Gold Iceberg `symbol_vwap_1m`    (hour partition OVERWRITE)
                                  │
                                  ▼
                              Athena ──▶ QuickSight
```

### 2.3 핵심 토폴로지 결정

**(a) Bronze→Silver 를 streaming이 아닌 micro-batch로 분리**
- (CLAUDE.md 결정) Bronze=Parquet (NOT Iceberg). streaming Iceberg sink는 MERGE semantics와 함께 production 검증 약함
- Iceberg MERGE는 transaction 단위. micro-batch마다 MERGE = snapshot/manifest 폭증
- 운영 가시성: micro-batch cycle = "Silver 신선도" SLO 지표 (Airflow UI 자연 노출)
- user 강점 살림: batch 단계가 Airflow가 통괄
- user 우려 격리: Streaming 책임이 "Kafka → file sink" (가장 단순한 path) 만으로 축소
- replayability: Bronze immutable raw → Silver 로직 버그를 나중에 발견해도 재처리 가능

**(b) Spark Standalone 한 클러스터에서 streaming + batch 함께**
- 로컬 Mac 자원 한계 (16-32GB) 에서 클러스터 2개 분리 시 OOM 가능성
- 영업시간 vs 장 마감 후 워크로드 비대칭: streaming idle 시간(매일 18시간)에 batch가 자원 흡수
- FairScheduler pool 분리 (`streaming_pool` 우선↑, `batch_pool` 양보 가능) 로 자원 충돌 완화
- 100x 진화 경로: EMR Serverless 시 streaming application + batch application 분리

### 2.4 Kafka Topic & Partition 설계

#### Topic 구조 — 이벤트 종류별 분리 (Phase 1 = 1 토픽, Phase 2부터 분리)

| Topic 이름 | Phase | 들어오는 이벤트 | 용도 |
|---|---|---|---|
| `kis.tick.raw` | **Phase 1 (현재)** | KIS H0STCNT0 실시간 체결가 | Bronze streaming source |
| `kis.quote.raw` | Phase 2 | KIS H0STASP0 실시간 호가 (bid/ask 10단계) | 호가 분석 별도 lakehouse 갈래 |
| `dart.disclosure.raw` | Phase 2 | DART 공시 이벤트 push (또는 polling 결과) | DART batch + alerting |

**왜 이벤트 종류별 분리 (vs 단일 토픽 `kis.raw` 에 모든 이벤트)**:
- ① **schema 안정성** — H0STCNT0 (체결) 와 H0STASP0 (호가) 은 schema가 다름. 한 토픽 안에 섞이면 consumer가 매번 type-discriminator 분기 필요. schema evolution 시 영향 범위 broad
- ② **consumer scaling 독립** — 체결과 호가는 traffic 양·처리 비용이 다름. 별 토픽이면 consumer parallelism·partitions를 각자 튜닝 가능
- ③ **retention 정책 독립** — 체결은 7일, 호가는 1일 (양 많고 가치는 즉시) 같은 차등화 가능
- ④ **Phase 분리와 align** — Phase 1 = `kis.tick.raw` 만, Phase 2에 `kis.quote.raw` 추가 = 토픽 단위로 evolution 명확

**왜 종목별 분리 (`kis.tick.raw.005930`, ..) 하지 않았나**:
- 종목 추가/삭제마다 토픽 생성/삭제 운영 부담
- Spark Streaming consumer가 토픽 수만큼 늘어남 (3 → 200 → ... 무한)
- 종목별 retention/scale 차등화 필요 거의 없음 — schema 동일, 처리도 동일
- → 종목 차원은 **partition** 으로 분리, 토픽은 **이벤트 종류** 로만 분리

#### Partition 설계 — `kis.tick.raw` 기준

| 항목 | 값 | 결정 이유 |
|---|---|---|
| **partitions** | **12** | Phase 1 3 종목 + Phase 2 30 종목 모두 cover. Kafka는 partition 늘리기는 가능하지만 줄이기 어려움 → 미래 여유 우선. 12 = 3 (Phase 1) + 9 buffer |
| **partition key** | **`symbol` (종목코드)** | 같은 종목 메시지 = 같은 partition (Kafka FIFO ordering 보장) |
| **replication factor** | **1** (Phase 1) | dev 환경. 운영 전환 시 RF=3 (아래 6.2 참조) |
| **min.insync.replicas** | 1 (Phase 1) | RF=1과 일관 |
| **retention** | 7일 (default) | Bronze로 적재되므로 Kafka는 buffer 역할만. 디버깅 시 1주일치 replay 가능 |

**partition key = `symbol` 선택 trade-off**:

| 옵션 | 장점 | 단점 |
|---|---|---|
| **`symbol`** ⭐ | 종목별 ordering 보장 → cum_volume monotonic 검증 가능. 디버깅 시 한 종목 흐름이 한 partition 안에 모임 (관찰 쉬움) | hot partition 가능성 (삼성전자가 NAVER 보다 5–10x 활발) |
| `null` (round-robin) | partition 균등 사용 | 종목 ordering 깨짐 → cum_volume monotonic 검증 어려움 |
| `hash(symbol \|\| ts_minute)` | 분산 + 분 단위 그룹핑 | 종목 내 ordering 부분만 보존, 디버깅 어려움 |

**왜 `symbol` 채택**:
- 우리 trade_uid 합성 키 = (symbol, trade_ts_kst, cum_volume) → cum_volume monotonic 가정. 종목 내 ordering 보장이 데이터 정합성 검증의 기반
- Phase 1 3 종목에서 hot partition 부담 무시 가능 (pico-scale)
- 100x 시 hot partition 깨지면 → key를 `(symbol, ts_minute_bucket)` 으로 진화. 이때 ordering 손실은 1분 윈도우 내에서만 → 여전히 dedup 가능

**100x 시 partition 설계 진화**:
- partitions 12 → 60–120 (종목 수 비례 + 처리량 비례)
- partition key = composite (symbol + minute bucket) 도입 검토
- replication factor 1 → 3, MSK Serverless 채택

---

## 3. 데이터 모델

### 3.1 Bronze — `bronze.kis_tick_raw` (Parquet, external)

KIS WebSocket H0STCNT0 payload 를 파싱했지만 dedup/필터링 안 한 raw fact. raw_payload string 도 보존 (audit·replay 보험).

```sql
CREATE EXTERNAL TABLE bronze.kis_tick_raw (
  ingest_ts          timestamp,           -- Spark Streaming write timestamp (UTC)
  kafka_partition    int,
  kafka_offset       bigint,
  symbol             string,              -- MKSC_SHRN_ISCD
  trade_ts_kst       timestamp,           -- STCK_CNTG_HOUR + 영업일 조립
  price              decimal(18,2),       -- STCK_PRPR
  volume             bigint,              -- CNTG_VOL
  cum_volume         bigint,              -- ACML_VOL
  cum_amount         bigint,              -- ACML_TR_PBMN
  trade_side         string,              -- CCLD_DVSN
  best_ask_price     decimal(18,2),       -- ASKP1
  best_bid_price     decimal(18,2),       -- BIDP1
  raw_payload        string               -- 원본 |-구분 텍스트 또는 JSON
)
PARTITIONED BY (dt date, hr int)
STORED AS PARQUET
LOCATION 's3://tickberg-lakehouse/bronze/kis_tick_raw/';
```

**파티션 (dt, hr) 결정 이유**: 영업일 09–15시 = 7개 hour 파티션/일. 다운스트림 5분 micro-batch가 "최근 1시간 파티션만 읽기" 식으로 incremental scan 단순. Glue partition projection 적용 → MSCK 불필요.

### 3.2 Silver — `silver.kis_tick_clean` (Iceberg v2)

정제된 체결 fact. dedup 후 영업시간 외 데이터 제거.

```sql
CREATE TABLE silver.kis_tick_clean (
  trade_uid       string,                  -- PK = symbol||'_'||trade_ts_kst||'_'||cum_volume
  symbol          string,
  trade_ts_kst    timestamp,
  trade_ts_utc    timestamp,
  price           decimal(18,2),
  volume          bigint,
  trade_amount    decimal(20,2),           -- price * volume
  trade_side      string,
  best_ask_price  decimal(18,2),
  best_bid_price  decimal(18,2),
  ingest_ts       timestamp,
  silver_ts       timestamp                -- Silver write timestamp (audit)
)
USING iceberg
PARTITIONED BY (days(trade_ts_kst), hour(trade_ts_kst))
TBLPROPERTIES (
  'format-version'='2',
  'write.distribution-mode'='hash',
  'write.target-file-size-bytes'='268435456'    -- 256 MB (write 시점 target)
);
```

**trade_uid 합성 키 결정 이유**: KIS H0STCNT0 stream에 명시적 trade-id 없음. 누적거래량(cum_volume)이 매 체결마다 monotonic 증가하므로 (symbol, trade_ts_kst, cum_volume) 조합이 사실상 unique. 정전·재접속 후 같은 row 재수신 시 trade_uid 같아 MERGE에서 자연 dedup.

**format-version=2 결정 이유**: position/equality delete file이 streaming 재시작·Bronze 중복 시 idempotent MERGE에 필요.

### 3.3 Silver — `silver.dim_symbol` (Iceberg, **SCD1 + time-travel**)

```sql
CREATE TABLE silver.dim_symbol (
  symbol             string,                -- PK
  symbol_name        string,
  market             string,                -- KOSPI / KOSDAQ
  par_value          decimal(18,2),         -- 액면가
  shares_outstanding bigint,
  is_active          boolean,               -- 상장폐지 시 false
  updated_ts         timestamp
)
USING iceberg
PARTITIONED BY (market);
```

**SCD2 대신 SCD1 + Iceberg time-travel 결정 이유**:
- SCD2 = effective_from/to/is_current 컬럼 3개 + 변경 감지 + 기존 row close + 새 row open MERGE 로직 복잡
- Iceberg 자체가 commit마다 snapshot 보존 → `TIMESTAMP AS OF` 1줄로 과거 상태 조회
- 트레이드오프: snapshot retention 기간 (30일 default) 너머는 history 사라짐 → 현재 학습용 audit (분기 단위 액면분할 등) 에는 충분. 영구 history 필요 시 retention 늘리거나 분기별 snapshot archive DAG 추가 (Phase 2)

### 3.4 Gold — `gold.symbol_vwap_1m` (Iceberg)

```sql
CREATE TABLE gold.symbol_vwap_1m (
  symbol         string,
  ts_minute      timestamp,                 -- 1분 시작 timestamp KST
  open_price     decimal(18,2),
  close_price    decimal(18,2),
  high_price     decimal(18,2),
  low_price      decimal(18,2),
  total_volume   bigint,
  vwap           decimal(18,4),             -- volume-weighted average price
  trade_count    int,
  computed_at    timestamp
)
USING iceberg
PARTITIONED BY (days(ts_minute), hours(ts_minute))
TBLPROPERTIES (
  'format-version'='2',
  'write.target-file-size-bytes'='268435456'    -- 256 MB
);
```

**5분마다 hour partition 전체 OVERWRITE 결정 이유**:
- minute 파티션 = 영업일 한 종목당 420 파티션/일 → small-files 폭주
- day 파티션 = 5분마다 그날치 전부 재계산 (비용·atomicity scope 부적절)
- hour 파티션 = 5분마다 60 row OVERWRITE (작음). atomicity scope 명확 ("hour 안에서 atomic, hour 밖은 안 건드림")

### 3.5 Iceberg 정당화 — 3 가치 ↔ 3 위치 매핑 (CLAUDE.md "Iceberg 핵심 결정")

| Iceberg 가치 | 어디서 시연 | 1차 발표 demo 시나리오 |
|---|---|---|
| ① MERGE INTO (액면분할·상폐) | `silver.dim_symbol` | 가상: 005930 par_value 5000→100, shares_outstanding 50배. MERGE 후 결과 확인 |
| ② OVERWRITE 원자성 (대시보드 일관성) | `gold.symbol_vwap_1m` | hour partition OVERWRITE 중 QuickSight refresh = partial read 없음 |
| ③ Time-travel (Audit Trail) | `silver.dim_symbol` | `TIMESTAMP AS OF '<MERGE 직전>'` vs 현재 비교 SQL |

**"Parquet+Glue로 안 되나?" 질문 답**:
1. atomic upsert 불가 → MERGE 못 함
2. HIVE 풀 overwrite는 mid-write race로 partial read 가능 → 대시보드 일관성 깨짐
3. snapshot 기반 time-travel 불가 → audit 시 Bronze 재처리 필요

이 3개가 본 모델에서 **실제로 필요한** capability — 평가에서 즉답.

### 3.6 데이터 규모 추정 (3 종목, 영업일 6.5h, 평균 ~45 trades/sec, 피크 ~150/sec)

종목별 일평균 추정 (보수): 삼성전자 ~60만, SK하이닉스 ~30만, NAVER ~10만 → **합계 ~100만 trades/일**.

| Layer | row 수 (영업일 1일) | 압축 후 크기 | 영업일 250일/년 |
|---|---|---|---|
| Bronze | ~1M | 100–170 MB/일 | 25–43 GB |
| Silver `kis_tick_clean` | ~1M | 50–90 MB/일 | 13–23 GB |
| Silver `dim_symbol` | ~3 | < 1 KB | < 1 MB |
| Gold `symbol_vwap_1m` | 1,170 (3×6.5×60) | < 1 MB | ~60 MB |

Phase 1 데모기간 (영업일 ~7일): Bronze ~1 GB, Silver ~0.5 GB, Gold ~1.5 MB.

**100x = 1억 trades/일** (CLAUDE.md "100만→1억" framework 와 정확히 align). Phase 1 baseline 1M/일이 100x base가 됨 → 발표 narrative 깔끔.

---

## 4. Pipeline & Orchestration

### 4.1 Spark Streaming `processingTime = 1 minute` 결정

후보 비교 (1시간 hour 파티션 기준 file 수):

| 옵션 | 1h file 수 | 신선도 | 채택 여부 |
|---|---|---|---|
| 10s | 360 | 10s | ❌ small-files 폭주 |
| 30s | 120 | 30s | ❌ Compaction 비용 큼 |
| **1m** ⭐ | **60** | **1–2m** | ✅ 분봉 Gold 1분 boundary와 align, Compaction 1회로 정리 |
| 2m | 30 | 2m | ❌ 1분 boundary 어긋남 |
| 5m | 12 | 5m | ❌ streaming 메시지 약함 (batch와 구분 안 됨) |

### 4.2 Airflow DAG 5개 (시간대 분리 명시)

**Airflow 설정**: `default_timezone = 'Asia/Seoul'` (`airflow.cfg`). 아래 모든 cron 표현은 **KST 기준**.

| DAG | 구간 | schedule (KST) | 동작 | retry / SLA |
|---|---|---|---|---|
| `bronze_to_silver_kis` | **장 시간대** | `*/5 9-16 * * MON-FRI` | 최근 10분 Bronze → Silver MERGE INTO | retries=2, SLA=10min |
| `silver_to_gold_vwap` | **장 시간대** | 위 DAG에 ExternalTaskSensor chain | 현재 hour partition OVERWRITE | retries=2, SLA=10min |
| `dim_symbol_daily` | **장 마감 후** | `0 4 * * *` | KIS REST → MERGE silver_dim_symbol | retries=3 |
| `iceberg_compaction` | **장 마감 후** | `0 18 * * MON-FRI` | Silver/Gold `rewrite_data_files` (target 384 MB, 256–512 MB 범위) | retries=1 |
| `expire_snapshots` (5/16) | **장 마감 후** | `0 19 * * SUN` | 30일 이전 snapshot 청소 | retries=1 |

**Spark Streaming 자체는 Airflow 외부**: long-running, task 모델과 안 맞음. compose `restart=unless-stopped` + Spark UI 메트릭 사용.

### 4.3 Idempotency 4단계 (재실행·디버깅 안전망)

| 단계 | 메커니즘 | 재실행 시 |
|---|---|---|
| Stream → Bronze | Spark checkpoint + Parquet append | checkpoint 손상 시 같은 offset 재처리 → 다음 단계가 흡수 |
| Bronze → Silver | `MERGE INTO ON trade_uid` | 같은 trade_uid 한 번만. 윈도우 겹쳐도 안전 |
| Silver → Gold | `INSERT OVERWRITE` per hour partition | 항상 같은 결과 (dynamic overwrite) |
| dim_symbol | `MERGE INTO ON symbol` | 종목별 latest 갱신, 멱등 |

### 4.4 시간대 분리 매트릭스

| 항목 | 장 시간대 (영업일 09:00–15:30 KST) | 장 마감 후 / 비영업일 (15:30 ~ 익일 09:00) |
|---|---|---|
| KIS Producer | active subscribe + Kafka publish | idle (메시지 0 = 정상) |
| KIS access_token refresh | — (장중 refresh 금지, §6.1.1) | **03:30 KST 매일 고정** |
| Spark Streaming | 1분 micro-batch active | idle |
| Bronze→Silver DAG | `*/5` 활성 | 비활성 |
| Silver→Gold DAG | `*/5` 활성 | 비활성 |
| dim_symbol 일배치 | — | 04:00 MERGE (token refresh 30분 후) |
| Compaction | — | 18:00 MON-FRI |
| expire_snapshots | — | 19:00 SUN (5/16 추가) |
| 자원 패턴 | streaming + batch peak (FairScheduler 분리) | batch only (Compaction 무거움) |
| 활성 모니터링 | T1 Grafana, T2 Airflow | T3 QuickSight, T4 ad-hoc SQL |

**Compaction window 안정**: 15:30 (장 마감) → 18:00 사이 buffer 2.5h. `*/5 cycle MERGE` 와 Compaction 절대 겹치지 않음.

### 4.5 코드 배치 (CLAUDE.md 디렉토리 contract)

```
code/pipelines/bronze/   bronze_kis_tick_streaming.py            (long-running)
code/pipelines/silver/   bronze_to_silver_kis_tick.py            (Airflow trigger)
                         dim_symbol_daily.py                     (Airflow trigger)
                         iceberg_compaction.py                   (Airflow trigger, multi-table)
                         expire_snapshots.py                     (5/16에 추가)
code/pipelines/gold/     silver_to_gold_vwap.py                  (Airflow trigger)
code/ddl/{bronze,silver,gold}/   *.sql
code/health-queries/             01–04*.sql (1차) + 05–07 (5/16)
orchestration/dags/              <DAG 5개> .py
infra/docker/kis-producer/       Dockerfile + main.py + requirements.txt
infra/docker/                    docker-compose.yml (부트캠프 base 적응)
infra/scripts/                   AWS 초기 셋업 스크립트
infra/terraform/                 (선택, 5/16에 IaC 정리)
```

---

## 5. 운영 가시성 — 4-tier + 5분 헬스체크

### 5.1 4-tier 구조 (audience·cadence 분리)

| Tier | 대상 | 주기 | 도구 | 무엇을 본다 |
|---|---|---|---|---|
| T1 실시간 | SRE/Dev | 1분 | Grafana | Kafka lag, Spark batch duration, KIS producer health |
| T2 파이프라인 | 데이터엔지니어 | 5–15분 | Airflow UI | DAG run, SLA miss, retry 이력 |
| T3 비즈니스 | 운영자/PM | 일·시간 | QuickSight 운영탭 | 종목 coverage, 신선도, throughput 추세 |
| T4 ad-hoc | 디버깅 시 | on-demand | Athena (`code/health-queries/`) | dedup rate, late arrival, partition 누락 |

**4계층 분리 정당화**: 한 화면에 다 몰면 5분 안에 답 못 찾음. 알람 → 어디 보고 → 어떻게 격리 의 decision tree가 도구별 분산되어야 빠른 진단.

### 5.2 5분 헬스체크 시나리오 (발표 narrative)

```
00:00 — Slack: [CRITICAL] Silver SLA miss: 12 min lag (Airflow SLA)
00:30 — Grafana "Kafka lag" 패널 → 정상 / 폭주 분기
        (정상 → Silver 단계 문제, T2로)  (폭주 → 상류 문제, KIS producer 패널로)
01:00 — Airflow UI: bronze_to_silver_kis 최근 run = retry/failed
        흔한 원인 3개: (1) Iceberg commit conflict, (2) AWS 자격 만료, (3) Spark OOM
02:00 — Health query 실행 (저장된 SQL):
        SELECT symbol, COUNT(*), MAX(trade_ts_kst) FROM silver.kis_tick_clean
          WHERE silver_ts >= current_timestamp - interval '15' minute
          GROUP BY symbol;
        → 3 종목 vs 2 종목 → 1 종목 누락 = KIS WebSocket 일부 구독 실패
03:00–05:00 — 원인별 액션 (KIS producer 컨테이너 restart 또는 escalate)
```

### 5.3 Grafana 패널 — 1차 (2개)

| 패널 | 메트릭 | 정상 | 경보 |
|---|---|---|---|
| Kafka topic `kis.tick.raw` lag | `kafka_consumergroup_lag{group="spark-streaming-bronze"}` | < 1,000 | > 10,000 |
| Spark Streaming batch duration | `spark_streaming_batch_duration_seconds` | < 30s | > 50s |

**5/16 추가**: KIS producer message rate, ws_connected, batch lag (DAG별), Athena query latency.

### 5.4 QuickSight 운영탭 — 1차 (3 viz)

| Visualization | 데이터 소스 | 의미 |
|---|---|---|
| Bronze freshness KPI | `MAX(ingest_ts)` Bronze | "데이터 N분 전 도착" |
| Symbol coverage Bar | 종목별 1h tick count | 3개 막대 (삼성전자/SK하이닉스/NAVER), 누락 즉시 발견 |
| Silver row count per 5min (24h) | Silver `silver_ts` | throughput 추세 + 영업시간 패턴 |

**5/16 추가**: Iceberg snapshot count over time, late arrival histogram, source coverage (DART/신용정보원).

### 5.5 Health queries — `code/health-queries/` (1차에 4개)

| 파일 | 검증 |
|---|---|
| `01_bronze_freshness.sql` | `MAX(ingest_ts) vs now` → lag 초 단위 |
| `02_silver_dedup_rate.sql` | Bronze count / Silver count 1h → 0.95–1.0 정상 |
| `03_symbol_coverage.sql` | 최근 15분 종목별 tick count → 3개 모두 (삼성전자/SK하이닉스/NAVER) |
| `04_gold_partition_completeness.sql` | 영업시간 hour 별 분봉 row 수 → 180 = 3×60 |

**5/16 추가**: `05_late_arrival_distribution`, `06_iceberg_snapshot_growth`, `07_compaction_file_reduction`.

---

## 6. Reliability & 비용

### 6.1 KIS Producer 안정성 (user 우려 #1)

| 실패 모드 | 대응 |
|---|---|
| WebSocket 일시 끊김 | exponential backoff 재접속 (1s → 60s cap), 무한 retry, `kis_ws_connected{}` 메트릭 |
| access_token 만료 (24h) | **고정 시점 03:30 KST 강제 갱신** (장중 refresh 절대 발생 안 함, 아래 정책 참조) |
| approval_key 만료 (1일, WebSocket 전용) | 03:30 KST 토큰 갱신 시 함께 새 발급 후 WebSocket 재접속 |
| heartbeat (PINGPONG) timeout | 60s 안 응답 없으면 강제 재접속 |

**구독 state 복구**: KIS WebSocket = connection-state. 재접속 시 종목 list 메모리 보관 → 자동 re-subscribe.

#### 6.1.1 access_token refresh 정책 — 장중 외 강제 (사용자 요구)

**원칙**: 장중(영업일 09:00–15:30 KST) 중 access_token refresh 가 떨어지면 일시적 connection drop · 데이터 손실 risk 발생. 따라서 **refresh는 항상 장 시작 전에 완료**되어야 함.

| 항목 | 값 |
|---|---|
| Refresh 시점 | **매일 03:30 KST 고정** (장 시작 5.5h 전 buffer) |
| Refresh window | 03:30 – 04:30 (1h). 실패 시 5분 간격 retry |
| Cutoff | 04:30. 4:30까지 실패하면 critical Slack/email alert |
| Refresh 빈도 | 매일 1회 (KIS access_token TTL = 24h, 매일 새벽에 강제 교체) |
| 영업일/비영업일 | 동일 — 매일 03:30. KIS API rate limit 무관 운영 단순화 |
| approval_key (WebSocket 전용) | 토큰 갱신 직후 함께 새 발급 → WebSocket 재접속 (한 번에 묶음) |

**구현 메커니즘**:
- KIS Producer 컨테이너는 다음 refresh 시각을 항상 **다음 03:30 KST** 로 계산 보관
- 만료 시각 추적 로직 불필요 — 매일 강제 교체이므로 token 잔여시간 무관
- 03:30이 영업일 09:00 의 5.5h 전 buffer라 token 갱신 실패 시 수동 대응 충분

**왜 03:30인가**:
- 새벽 시간대라 KIS API 부하 적음 (다른 사용자도 새벽 갱신 안 함)
- `dim_symbol_daily` DAG 04:00 보다 30분 앞서 → token이 dim_symbol DAG 의 KIS REST 호출에도 사용 가능 (다음 영업일 마스터 갱신)
- 장 시작 09:00 까지 5.5h buffer — 04:30 cutoff 후에도 4.5h 동안 수동 대응 가능

### 6.2 Kafka Producer 설정

```python
KafkaProducer(
    acks='all',
    enable_idempotence=True,
    retries=10, retry_backoff_ms=100,
    linger_ms=50,
    compression_type='snappy',
    max_in_flight_requests_per_connection=5
)
```

**Replication factor 정책**:

| 환경 | RF | min.insync.replicas | broker 수 | 사유 |
|---|---|---|---|---|
| **Phase 1 (현재, dev)** | **1** | 1 | 1 | 로컬 단일 broker. 학습·발표용. broker 죽으면 데이터 손실 가능하지만 Bronze 적재 후 손실 외에 큰 영향 없음 |
| **Phase 1 (100x evolution dev)** | 1 | 1 | 1 | 여전히 dev — 100x 시뮬도 단일 broker 기준 (스코프 제한) |
| **실제 운영 (Phase 2 운영 전환 시)** | **3** | **2** | 3+ | broker 1대 죽어도 가용성 유지 (RF=3 + ISR ≥ 2 = 1대 손실 허용). MSK Serverless 또는 self-managed 3-broker cluster |

→ Phase 1 / 100x evolution 모두 **RF=1 유지**. RF=3 으로의 전환은 "운영 전환" 이라는 별도 사건이며 본 spec scope 밖. **5/16 발표에서 "왜 RF=1?" 질문 시 답**: "dev 환경 단순화 우선. 운영 전환 시 RF=3 + min.insync.replicas=2 + MSK 채택." (이 문장만으로 충분).

### 6.3 Spark Structured Streaming checkpoint

- Location: `s3://tickberg-lakehouse/checkpoints/bronze_kis_tick/`
- Spark 3.5+ Kafka source + file sink = commit log 기반 exactly-once write
- checkpoint 손상 = 재처리 = Bronze 중복 → Silver MERGE 멱등성이 마지막 안전망

### 6.4 Iceberg commit 충돌 방지

- Airflow `max_active_runs=1` → 같은 DAG task 2 instance 동시 실행 차단
- Bronze→Silver와 Silver→Gold = 다른 테이블 → 충돌 없음
- Silver `kis_tick_clean` 동시 commit 가능자: ① MERGE (장 시간대), ② Compaction (장 마감 후 18:00) — **시간 분리** 로 충돌 없음
- Iceberg 자체 retry (concurrent commit retry 4회) 가 fluctuation 흡수

### 6.5 Airflow 정책

| 항목 | 값 |
|---|---|
| `retries` | 2 |
| `retry_delay` | 1 min |
| `sla` | 10 min |
| `max_active_runs` | 1 |
| `email_on_failure` | True |

### 6.6 AWS 비용 가드레일 (CLAUDE.md 명시)

| 가드레일 | 구현 |
|---|---|
| S3 Lifecycle | `bronze/` → 90일 후 Glacier IR |
| Athena workgroup | `BytesScannedCutoffPerQuery=5GB` |
| AWS Budgets | 월 $20 초과 email |
| Glue 무료티어 모니터링 | CloudWatch metrics |
| Iceberg expire-snapshots | 30일 이전 청소 (5/16 추가) |
| 데모 후 manual cleanup | `aws s3 rm` |

### 6.7 비용 추정 — worst case (보수적)

**Phase 1 데모기간 (영업일 ~7일)**:

| 서비스 | 보수 가정 | 비용 |
|---|---|---|
| S3 storage | 압축 효율 2x 보수 + Iceberg metadata 누적 5x → ~10 GB (3종목 baseline) | $0.25/월 |
| S3 PUT/GET | Iceberg commit + Spark checkpoint = ~5K PUT/일 × 7일 (file 수는 종목 수와 무관) | $0.20 |
| Athena query | 디버깅 부주의 scan = 5 GB scan/주 | $0.03 |
| Glue Catalog | 무료 한도 안 | $0 |
| QuickSight | Author license 필요 (대시보드 작성). Standard Author $24/user/월 (1명 가정). 첫 30일 free trial 만료 가정 | $24 |
| Cross-AZ data transfer | QuickSight ↔ Athena | $0.50 |
| **Phase 1 worst case** | | **~$24–26/월** (S3 비중 작아져 약간 감소) |

**100x (3억 trades/day) — worst case**:

| 카테고리 | 월 비용 |
|---|---|
| EMR Serverless 컴퓨트 | $1,300–2,700 |
| MSK Serverless | $50–100 |
| S3 storage (1.5 TB) + PUT | $145 |
| QuickSight Enterprise (5 users mix Author/Reader) | $100–160 |
| Athena scan (1 TB/월) | $5 |
| Glue 유료 | $2 |
| **100x worst case 합계** | **$1,600–3,100/월** |

> **가격 disclaimer**: 위 추정은 2026 AWS ap-northeast-2 공시 가격 기준. QuickSight Author/Reader mix, EMR Serverless DPU·시간, SPICE 사용량에 따라 ±30% 변동 가능. worst case framing 자체가 핵심.

**핵심 메시지**:
- Phase 1 worst case ($25–27/월) 도 **AWS Budgets $20 alarm을 살짝 초과** → 첫 month에서 alarm 트리거 → narrative ("budget alarm 동작 검증" 으로 활용)
- 100x worst case ($1,600–3,100/월) → budget **80–155x 초과**. "깨지는 곳은 storage가 아니라 컴퓨트 + QuickSight license". budget 10–100x 늘리거나 EMR Serverless on-demand 컴퓨트로 영업시간만 가동, QuickSight reader/author mix 최적화

### 6.8 Secret management

- `.env` 만, git 커밋 금지 (`.env.example` 만)
- AWS 자격: `AWS_PROFILE=tickberg` (`~/.aws/credentials`)
- IAM 사용자 최소 권한 (S3 단일 버킷 + Glue DB + Athena workgroup)
- Phase 2 운영 전환 시 AWS Secrets Manager

### 6.9 발표 demo 안정성 — 30분 전 sanity check

```
T-30min  KIS Producer 컨테이너 재시작 (clean state)
T-28min  Grafana — Kafka lag 정상 확인
T-25min  Athena 쿼리 — 직전 영업일 Bronze 데이터 확인
T-20min  Airflow — 최근 DAG run success
T-15min  QuickSight 새로고침 → 운영탭 정상
T-10min  발표 슬라이드 + 녹화 영상 + browser tab 정리
```

---

## 7. Test 전략 + Fallback

### 7.1 결정적 사실 — 두 발표일 = 비영업일

- 5/10 일 = **비영업일** (직전 영업일 5/8 금, 차기 영업일 5/11 월)
- 5/16 토 = **비영업일** (직전 영업일 5/15 금, 차기 영업일 5/18 월)

→ 두 시점 모두 KIS WebSocket 실시간 데이터 0. demo는 직전 영업일 적재 데이터 + 녹화 영상으로 진행.

### 7.2 Demo strategy — (A) 보존 데이터 + 녹화 영상 채택

| 발표 | 녹화 대상 영업일 | 녹화 시점 | 녹화 화면 |
|---|---|---|---|
| 5/10 1차 | 5/8 금 | 영업시간 5–10분 (09:30 또는 14:30) | Grafana 패널, Spark Streaming UI, Airflow 5분 cycle, Athena 쿼리 갱신 |
| 5/16 최종 | 5/15 금 | 동일 | 위 + 운영탭 풍부화 QuickSight |

**Replay tool 없음**. 녹화로 대체 → 발표 중 streaming 끊김 risk 0.

발표 narrative: "비영업일이라 streaming idle = 정상. 장 시간대 동작은 녹화 영상으로." → "장 시간대 vs 장 마감 후 분리" 일관성.

### 7.3 Test 자산 — 1차 (~6.5h)

| 테스트 | 도구 | 검증 | 노력 |
|---|---|---|---|
| KIS payload parser unit | pytest, sample fixture 5–10 | KIS H0STCNT0 → Bronze schema 매핑, edge case (negative price, null trade_side, missing field) | 2h |
| Iceberg MERGE 통합 (= demo prep) | Spark local + Iceberg Hadoop catalog | dim_symbol 가상 액면분할 → MERGE → time-travel before/after assert. 발표 시연 SQL 그대로 | 3h |
| Health queries 사전 검증 | Athena | 4개 query 5/8 데이터로 실행해 정상 임계값 확정 | 1h |
| 30분 전 smoke checklist | manual (§6.9) | E2E 1회 통과 | 30min |

**"Iceberg MERGE 통합 테스트 = demo prep 겸용" 의미**: MERGE 검증 + 발표 시연이 같은 5단계 SQL. 한 번에 두 목적. 테스트 통과 = demo 안 깨짐 보장.

### 7.3.1 5/16 추가 테스트

| 테스트 | 노력 |
|---|---|
| Spark Streaming 통합 (testcontainers Kafka + S3 mock) | 4h |
| E2E replay 검증 (Bronze 샘플 → Silver/Gold 기대값) | 3h |
| Health queries unit-as-data-quality (Phase 2 dbt bridge) | 6h |
| 부하 시뮬 (replay 가속, bottleneck 식별) | 2h (시간 부족 시 cut 1순위) |

### 7.4 5/10 Risk → Fallback (보수적 cutoff, 5/9 토 21:00 이전 종료)

| Risk | Cutoff | Fallback |
|---|---|---|
| Iceberg MERGE 첫 사용 막힘 | **5/8 금 22:00** | Silver `INSERT INTO` (append-only) 강등. dedup → Gold 단계. MERGE 시연 슬라이드 = "5/16에 풀 구현". 5/16 복구 |
| Compaction DAG 미완성 | **5/9 토 12:00** | placeholder DAG (manual trigger only, schedule None). 발표 메시지 "야간 자동화 예정". 5/16 cron 활성화 |
| Streaming 발표 중 보여줄 게 없음 | (design 단계 결정) | 옵션 (A) 채택 — 녹화 영상 사용 |
| QuickSight refresh 지연 | **5/9 토 21:00** (발표 23h 전) | Athena 결과 미리 export → 슬라이드 캡처 + CSV 차트 |
| AWS 비용 budget alert | event-driven 즉시 | Athena workgroup 일시 중단 + S3 lifecycle 강화 + debug query 감사 |
| KIS 토큰 갱신 실패 | smoke 시점 | 수동 재발급. 비영업일이라 시연 영향 0 |

**5/10 일 (발표 당일) 의도된 활동**: 슬라이드 정제, 녹화 편집, narrative 리허설, 19:30 smoke, 20:00 발표. **코드 commit 0건**.

### 7.5 5/16 복구 plan

5/10 fallback 활성화한 항목들을 5/11–5/15 영업일 5일 동안 복구:

| 5/10 fallback | 5/16 복구 |
|---|---|
| Iceberg MERGE 강등 | 5/15 금 22:00 까지 MERGE INTO 복구 + 통합 검증 |
| Compaction placeholder | cron 활성화 + 5/13 수 첫 자동 실행 검증 |
| QuickSight Athena export | refresh 자동화, 운영탭 풍부화 |

5/16 cutoff 도 동일 원칙: **5/15 금 22:00** 까지 모든 fallback 결정 종료. 5/16 토 = 슬라이드 + smoke + 발표만.

---

## 8. 6일 분배 + 100x Scale Narrative

### 8.1 6일 캘린더

| 날짜 | 시스템 자동 (장 시간대) | 시스템 자동 (장 마감 후) | user 수동 (8h/일) |
|---|---|---|---|
| 5/11 월 (영업) | KIS 적재 + 5분 cycle batch | 18:00 Compaction · 04:00 dim_symbol | 5/10 fallback 복구 + DART 데이터 모델·DDL 설계 |
| 5/12 화 (영업) | 동일 | 동일 | DART API client + Bronze 적재 DAG + Silver MERGE |
| 5/13 수 (영업) | 동일 | 동일 | 신용정보원 xlsx 파서 + Bronze + expire_snapshots DAG |
| 5/14 목 (영업) | 동일 | 동일 | Grafana 4 패널 + QuickSight 운영탭 풍부화 + Health queries 3개 + 100x design doc 초안 |
| 5/15 금 (영업, 마지막 E2E) | 동일 + **오후 5–10분 녹화** | 동일 | am: Spark Streaming 통합 테스트 + E2E replay 검증 / pm: 100x design doc 완료 + 슬라이드 + **22:00 모든 cutoff 종료** |
| 5/16 토 (비영업, 최종 발표) | (영업 외) | — | 슬라이드 마무리 + smoke + 발표 |

### 8.2 Cut 우선순위 (시간 부족 시)

1. 부하 시뮬 (replay 가속) — design doc으로 cover
2. E2E replay 검증 자동 테스트
3. 신용정보원 Silver/Gold (Bronze까지로 단축)
4. DART Silver MERGE (Bronze + Athena 직접 쿼리로 단축)
5. QuickSight 운영탭 풍부화

### 8.3 100x scale dimension 4개 분해

#### Dimension 1: Throughput

| 깨짐 | 100x 시 증상 | 해결 (cost) |
|---|---|---|
| KIS WebSocket 1 conn/appkey ~40 종목 한도 | 200+ 종목 못 받음 | 멀티 appkey 분할 (5채널). 추가 비용 0~법인계정 |
| Spark Streaming 단일 worker (Phase 1 ~45/sec 평균, ~150/sec 피크) | 100x = 4,500/sec 평균, ~15K/sec 피크 → 단일 worker 한계 초과 | EMR Serverless streaming application 분리 + executor auto-scaling. **컴퓨트 ~$16K–32K/년 worst case** |
| Kafka single broker RF=1 | broker 죽음 = 손실 | MSK Serverless + RF=3 + min.insync.replicas=2. ~$50–100/월 |

#### Dimension 2: Batch Window

| 깨짐 | 증상 | 해결 |
|---|---|---|
| Bronze→Silver MERGE 5분 안 못 끝남 | Phase 1 = 5분당 ~13K rows × 100 = **~1.3M rows / 5min batch** → 단일 worker로 시간 초과 | (a) 종목별 partition 병렬, (b) trigger 5→2분으로 batch 작아짐, (c) Spark Streaming foreachBatch 분리 |
| Silver→Gold OVERWRITE 부담 | 동일 | Gold cascade (1초 → 1분 → 5분 → 1시간) |

#### Dimension 3: Storage / Compaction (장 마감 후)

| 깨짐 | 증상 | 해결 |
|---|---|---|
| Compaction 야간 window (3h) 초과 | 100x file 양 → 시간 부족 | day partition 분할 Compaction + RewriteManifests + write target file size↑ |
| expire_snapshots 못 따라잡음 | 5K+ snapshots/주 | 일배치 + retention 30→14일 |
| S3 storage worst case | 1.5 TB/월 → $125 | Lifecycle Bronze→Glacier IR 적극 (90→30일) |

#### Dimension 4: Concurrency

| 깨짐 | 증상 | 해결 |
|---|---|---|
| Athena 5GB workgroup | 대시보드 쿼리 초과 | (a) Gold 사전 집계 강화, (b) tier-2 workgroup, (c) result reuse |
| QuickSight SPICE 10GB | refresh 자동 실패 | Enterprise + 데이터셋 분할 |
| Glue Catalog 100만 req/월 | 100x = 3M req | 유료 ($1/M) |

### 8.4 100x worst case 비용 종합 — §6.7 참조 (~$1,600–3,100/월, budget 80–155x 초과). Phase 1 worst case도 budget alarm 살짝 초과 → "alarm 동작 검증" narrative

### 8.5 Evolution 경로 (한 번에 100x 가지 않음)

```
Phase 1 (1x  =  100만/일):  로컬 Docker · 3 종목 (삼성전자/SK하이닉스/NAVER)
       ↓
Phase 2 (10x = 1천만/일):   KOSPI 시총 상위 30 · MSK · Spark Streaming local 유지
       ↓
Phase 3 (50x = 5천만/일):   KOSPI200 · EMR Serverless streaming · Iceberg branch 활용
       ↓
Phase 4 (100x = 1억/일):    전 종목 (KOSPI + KOSDAQ) · 멀티 region · MSK + EMR Serverless · QuickSight Enterprise
```

CLAUDE.md "100만 → 1억" framework 와 정확히 align. 각 단계에서 **어디가 먼저 깨지는지** 명확 → 단계적 진화 narrative.

### 8.6 5/16 최종 발표 구조 (10–12분)

```
1. 동기·결정 (1min)        한국 주식 lakehouse · AWS 단일 환경 · 메달리온
2. 시스템 시연 (3min)       5/15 녹화 영상 + Athena live + QuickSight (KPI탭+운영탭)
3. Iceberg 정당화 (2min)    MERGE 액면분할 + OVERWRITE 원자성 + time-travel
4. 운영 가시성 (2min)       4-tier 구조 + 5분 헬스체크 시나리오
5. 100x Scale 사고 (2min)   dimension 4 + worst-case 비용 + 진화 경로
6. Phase 2 로드맵 (1min)    dbt · 자동매매 · MSK · EMR Serverless
```

평가 4가지 ↔ 슬라이드 매핑:
- 운영 가시성 → 슬라이드 4
- 100x scale → 슬라이드 5
- Iceberg 필요성 → 슬라이드 3
- 협업·지속가능성 → 슬라이드 6 + CLAUDE.md + 본 design doc

---

## 9. Open questions / Phase 2 deferred

| 항목 | 사유 |
|---|---|
| dbt Semantic Layer | CLAUDE.md Phase 2. PySpark + SQL DDL로 Phase 1 충분 |
| 자동매매 (Signal/Order/Risk) | CLAUDE.md Phase 2. `code/pipelines/trading/` 빈 채로 두고 `TRADING_ENABLED=false` 유지 |
| Alpaca / 미국 / 암호화폐 | CLAUDE.md Phase 2 |
| EMR Serverless / MSK / Flink | 100x 진화 경로에만 명시, Phase 1 미사용 |
| Trino | Phase 1 미사용 (Athena로 충분), 필요 시 Phase 2 |
| Replay tool | (B) 옵션 — 녹화로 대체. 필요 시 Phase 2 |
| Great Expectations / dbt test | Phase 2 (5/16 health-queries 가 baseline) |
| AWS Secrets Manager | 운영 전환 시 (Phase 2) |
| SCD2 영구 history | snapshot retention 늘리거나 분기별 archive DAG (Phase 2) |

---

## Decision Log — 핵심 트레이드오프

| # | 결정 | 후보 | 선택 | 이유 |
|---|---|---|---|---|
| D1 | Phase 1 데이터 소스 범위 | (A) 한투만 E2E / (B) 한투+DART / (C) 3소스 모두 | **A** | 3일 budget + 신규 컴포넌트 risk 분산. 한 줄기 narrative |
| D2 | 종목 universe | 1–3 / 10 / 40 / KOSPI200 | **3 (삼성전자·SK하이닉스·NAVER)** | 일 100만 trades (≈관찰에 충분), CLAUDE.md "100만→1억" 100x baseline 정확 align, 학습용 demo 안정성 우선. user 결정으로 10에서 3으로 축소 |
| D3 | Bronze 포맷 | Iceberg / **Parquet** | **Parquet** | CLAUDE.md 결정 — streaming snapshot/manifest 오버헤드 회피 |
| D4 | Bronze→Silver 처리 | streaming-only / single-job 두 sink / **batch 분리** / Bronze 생략 | **batch 분리** | Iceberg MERGE = transaction batch에 자연. user 강점(Airflow) 살림. replayability 보존 |
| D5 | Spark cluster | 클러스터 2개 / **단일 + FairScheduler** / streaming만 클러스터 | **단일 + FairScheduler** | 로컬 자원 한계. 워크로드 시간 비대칭 활용. 100x 진화 경로 명확 |
| D6 | Spark Streaming trigger | 10s / 30s / **1분** / 2분 / 5분 / Continuous | **1분** | 분봉 Gold 1분 boundary align. Compaction 1회로 정리. streaming 메시지 유지 |
| D7 | dim_symbol SCD | SCD1 단순 / **SCD1 + Iceberg time-travel** / SCD2 | **SCD1 + time-travel** | SCD2 컬럼·MERGE 복잡도 → Iceberg engine 위임. CLAUDE.md "Time-travel = Audit" 일관 |
| D8 | trade_uid 합성 키 | 단일 컬럼 PK / **(symbol, trade_ts_kst, cum_volume) 합성** / hash | **합성 PK** | KIS API 자체에 명시 trade-id 없음. cum_volume monotonic 활용 |
| D9 | Gold OVERWRITE 단위 | minute partition / **hour partition** / day partition | **hour partition** | small-files 회피 + atomicity scope 명확. 5분당 60 row OVERWRITE |
| D10 | Iceberg 매니지먼트 자동화 1개 | Compaction / Expire / Orphan | **Compaction** (1차) + Expire (5/16) | Spark Action + 야간 Airflow DAG. 사용자 강점 활용 |
| D11 | Demo strategy (비영업일 발표) | (A) 보존 데이터 + **녹화** / (B) Replay tool / (C) Mock generator | **(A) + 녹화** | 발표 중 끊김 risk 0. user 시간 budget 절약. "장 시간대 vs 장 마감 후" narrative 일관 |
| D12 | Cutoff 보수도 | 발표 24h 전 / **48h 전** / 5/9 토 21:00 (모든 fallback 종료) | **5/9 토 21:00** | 발표 당일 코드 commit 0. 안전 마진 충분 |
| D13 | 100x 비용 framing | 평균값 / **worst-case + budget 초과 명시** | **worst-case 명시** | user 명시 지시 (memory/feedback). 평가에서 정직 = plus |
| D14 | Test 전략 | 풀 자동 E2E / **단위 + demo prep 겸용** / manual only | **단위 + 겸용** | 3일 budget에서 실효성 우선. 5/16에 통합 보강 |
| D15 | KIS access_token refresh 시점 | 만료 시각 추적 (가변) / **고정 시점 03:30 KST** | **03:30 KST 고정** | 장중(09:00–15:30) 중 refresh 절대 발생 안 함. 만료 추적 로직 단순화. 04:30 cutoff 후 4.5h buffer로 수동 대응 |
| D16 | Iceberg target file size | 128MB (write) only / **256MB (write) + 384MB (compaction)** | **256MB write + 384MB compaction** | 사용자 요구 256–512MB 범위. Athena/Spark scan 효율 + S3 큰 file 효율. Compaction이 hour 파티션 small files 1개로 합침 |
| D17 | Kafka topic 분리 | 단일 토픽 (`kis.raw`) / **이벤트 종류별 토픽** / 종목별 토픽 | **이벤트 종류별** (Phase 1 = `kis.tick.raw` 1개, Phase 2부터 `kis.quote.raw` 등 추가) | schema 안정성 + consumer scaling 독립 + Phase evolution과 align. 종목 차원은 partition으로 분리 (운영 단순화) |
| D18 | Kafka partition key | **`symbol`** / `null` round-robin / composite hash | **`symbol`** | trade_uid 의 cum_volume monotonic 검증을 위해 종목별 ordering 필수. 디버깅 가시성. hot partition은 100x 시 composite key로 진화 |
| D19 | Kafka replication factor | **1 (Phase 1 dev)** / 3 (운영) | **1 (Phase 1 + 100x evolution)** | dev 환경 단순화. 운영 전환은 별도 사건이며 spec scope 밖. 발표에서는 "운영 전환 시 RF=3 + ISR=2" 한 줄로 |

---

## 부록 A — Phase 1 핵심 파일 트리 (5/16 시점 예상)

```
tickberg/
├── CLAUDE.md
├── README.md
├── .env.example
├── docs/
│   └── superpowers/specs/
│       └── 2026-05-07-tickberg-phase1-mvp-design.md  ← 본 문서
├── code/
│   ├── ddl/
│   │   ├── bronze/  kis_tick_raw.sql
│   │   ├── silver/  kis_tick_clean.sql, dim_symbol.sql
│   │   └── gold/    symbol_vwap_1m.sql
│   ├── pipelines/
│   │   ├── bronze/  bronze_kis_tick_streaming.py
│   │   ├── silver/  bronze_to_silver_kis_tick.py
│   │   │           dim_symbol_daily.py
│   │   │           iceberg_compaction.py
│   │   │           expire_snapshots.py        (5/16)
│   │   ├── gold/    silver_to_gold_vwap.py
│   │   └── trading/  (빈 디렉토리, Phase 2)
│   └── health-queries/
│       ├── 01_bronze_freshness.sql
│       ├── 02_silver_dedup_rate.sql
│       ├── 03_symbol_coverage.sql
│       ├── 04_gold_partition_completeness.sql
│       ├── 05_late_arrival_distribution.sql        (5/16)
│       ├── 06_iceberg_snapshot_growth.sql          (5/16)
│       └── 07_compaction_file_reduction.sql        (5/16)
├── orchestration/
│   └── dags/
│       ├── bronze_to_silver_kis.py
│       ├── silver_to_gold_vwap.py
│       ├── dim_symbol_daily.py
│       ├── iceberg_compaction.py
│       └── expire_snapshots.py                     (5/16)
├── infra/
│   ├── docker/
│   │   ├── docker-compose.yml
│   │   └── kis-producer/
│   │       ├── Dockerfile
│   │       ├── main.py
│   │       └── requirements.txt
│   └── scripts/
│       └── aws_initial_setup.sh
├── monitoring/
│   ├── grafana/  dashboards/*.json
│   └── prometheus/  prometheus.yml
└── dashboard/
    └── quicksight/  (export JSON, 5/16)
```
