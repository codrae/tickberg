# tickberg — Real-time Korean Stock Tick Lakehouse

tickberg 최종 발표 자료. 33장 슬라이드, 발표 25분. Gamma 입력용 단일 MD. 슬라이드 구분은 `---`.

---

## 01. 표지

> tickberg — Real-time Korean Stock Market Tick Data Lakehouse

- 메타코드 DE 부트캠프 8회차 최종 프로젝트
- 한국투자증권 실시간 체결 + DART + 신용정보원
- S3 Iceberg Lakehouse + Superset + Prometheus/Grafana
- 발표자 / 발표일 2026-05-16

**시각자료**: 표지 — 프로젝트명 + 한 줄 소개 + 발표자명. 디자인은 Gamma 단계에서.

**Speaker Note:**
안녕하세요. 오늘 발표할 프로젝트는 tickberg 입니다.
한국 주식 실시간 체결 데이터를 메달리온 Lakehouse 로 적재하고 BI·운영 가시성으로 묶는 데이터 플랫폼입니다.
한국투자증권 Open API 의 실시간 체결가, DART 공시, 신용정보원 데이터 세 종을 다룹니다.
이걸 Bronze, Silver, Gold 세 계층으로 분리해서 적재하는 게 Phase 1 의 핵심입니다.
오늘 25분 동안 핵심 결정 5가지, Iceberg 가 왜 필요했는지, 운영을 어떻게 보고 있는지를 보여드리겠습니다.
마지막에 100배 트래픽 시나리오에 대한 사고 흐름과 Phase 2 로드맵을 공유하겠습니다.

---

## 02. 문제·도메인·청중 KPI

> 한국 주식 실시간 체결을 BI·운영 가시성과 함께 안전히 적재하기 어렵다.

- 도메인 : 분당 수만 건 체결 tick, 장 시간 vs 장 마감 후 부하 차이 100x
- 청중 : 평가 4축 — 운영 가시성 / 100x 사고력 / Iceberg 필요성 / 협업 지속성
- 목표 KPI : Bronze lag < 2분 (장 시간) / Silver dedup 0.95–1.0 / Gold 분봉 결손 0
- Out of Scope : dbt, 자동매매, 미국 주식, EMR Serverless (Phase 2)

**시각자료**: 평가 4축 표 2×2 + KPI 3종 가로 카드.

**Speaker Note:**
이 프로젝트가 풀려는 문제는 단순합니다. 한국 주식 실시간 체결 데이터를 안전히 쌓고, 그걸 BI 와 운영 모두에서 보게 하는 것입니다.
어려운 이유는 두 가지인데요. 첫째, 장 시간과 장 마감 후의 데이터 부하 차이가 100배가 넘어서 동일한 인프라로 두 구간을 처리할 수 없습니다.
둘째, 시점성과 정확성이 모두 중요해서 그냥 흘려 넣는 append 만으로는 부족하고 MERGE 와 Time-travel 이 필요했습니다.
평가는 운영 가시성, 100x 스케일 사고력, Iceberg 필요성, 협업·지속성 네 축으로 들어옵니다.
오늘 33장의 슬라이드는 이 네 축에 어떻게 답하는지 순서대로 보여드립니다.
KPI 는 Bronze lag 2분 이내, Silver dedup 비율 0.95 이상, Gold 분봉 결손 0 입니다. 평가 기준이 아니라 운영 기준입니다.

---

## 03. Data Source 3종 + 규모/EDA

> KIS WebSocket (실시간 tick) + DART (공시) + 신용정보원 (월별 증강) 3종.

- KIS Open API : WebSocket H0STCNT0 46-field, 분당 <TODO: 실측 tick/min>, REST 종목 마스터
- DART 공시 : 평일 일배치 06:00 KST, 분기 재무제표 + 주요공시
- 신용정보원 : 월 1회 xlsx, 신용위험 지표 (증강)
- EDA 결과 : 종목당 일평균 tick <TODO>, 가격 분포 <TODO>, 결측 패턴 <TODO>

**시각자료**: 3열 비교표 — Source / 갱신 주기 / 페이로드 / Phase 1 활용 지점.

**Speaker Note:**
데이터 소스는 셋입니다.
첫째, 한국투자증권 Open API. 실시간 WebSocket 으로 H0STCNT0 라는 46개 필드 체결 데이터를 받고, REST 로 종목 마스터를 가져옵니다.
둘째, DART 공시 API. 평일 아침 6시에 일배치로 분기 재무제표와 주요공시를 끌어옵니다. 장 시간에 돌리지 않는 이유는 자원을 KIS 스트리밍에 양보하기 위해서입니다.
셋째, 신용정보원 데이터. 월별 xlsx 라 batch 한 번이면 충분해서 증강 용도로만 씁니다.
EDA 결과 수치는 발표 직전 Athena 조회로 채워 넣을 예정입니다. 일평균 tick 수와 분포, 그리고 결측 패턴 세 가지를 살펴보고 Silver dedup 룰을 설계했습니다.

---

## 04. 아키텍처 한 장

> 컴퓨트는 로컬 Mac Docker, 데이터·카탈로그·BI 는 모두 AWS — 단일 환경 원칙.

- 로컬 : Kafka / Spark / Airflow / Prometheus / Grafana / Superset
- AWS (ap-northeast-2) : S3 / Glue Catalog / Athena workgroup
- 흐름 : KIS WebSocket → Kafka → Spark Streaming Bronze → Silver MERGE → Gold OVERWRITE → Athena → Superset
- 원칙 : "로컬에선 됐는데 AWS 에선 안 돼" 디버깅 차단

**시각자료**: 핸드오프 §3 의 데이터 흐름 다이어그램을 가로형 박스로 재구성. 좌 = 로컬, 우 = AWS, 점선 = 모니터링 평행선.

**Speaker Note:**
아키텍처는 한 가지 원칙으로 압축됩니다. 컴퓨트만 로컬 도커, 데이터·카탈로그·BI 는 전부 AWS 입니다.
보통 부트캠프 프로젝트는 비용을 아끼려고 로컬에 MinIO 와 Hive Metastore 를 띄우는데, 저는 일부러 그걸 안 썼습니다.
이유는 두 가지인데요. 첫째, 로컬에서 됐는데 AWS 에서 안 되는 디버깅을 한 번도 겪고 싶지 않았고, 둘째, 발표 시연을 라이브로 하려면 동일한 환경이어야 했습니다.
좌측은 로컬 도커 스택입니다. Kafka, Spark, Airflow, 그리고 모니터링용 Prometheus/Grafana 와 Superset.
우측은 AWS 입니다. 데이터는 S3, 카탈로그는 Glue, 쿼리는 Athena workgroup 으로 5GB 스캔 컷오프를 걸어 비용 가드레일을 만들었습니다.
점선으로 표현한 모니터링 평행선은 운영 가시성 슬라이드에서 다시 다룹니다.

---

## 05. 핵심 결정 5가지

> Bronze=Parquet / Silver·Gold=Iceberg / AWS-only / Athena DDL / dbt·자동매매 Phase 2.

- ① Bronze = Parquet (Iceberg X) — streaming snapshot/manifest 갱신 오버헤드 차단
- ② Silver/Gold = Iceberg — MERGE, OVERWRITE 원자성, Time-travel 세 가치
- ③ AWS 단일 환경 + ④ DDL = Athena SQL — Glue Catalog 일원화, `.sql` 단일 진실원
- ⑤ dbt·자동매매 = Phase 2 — Phase 1 은 Lakehouse + 운영 가시성에 집중

**시각자료**: 5행 표 — 결정 / 근거 / Trade-off 명시.

**Speaker Note:**
이 다섯 가지가 발표의 척추입니다.
첫째, Bronze 는 Parquet 입니다. Iceberg 가 아닙니다. 1분 trigger streaming 에서 snapshot 과 manifest 를 매번 갱신하면 그게 그대로 오버헤드입니다. 중복 제거나 UPSERT 는 어차피 Silver MERGE 한 번에 처리하면 충분합니다.
둘째, Silver 와 Gold 는 Iceberg 입니다. 종목 마스터에 액면분할이 들어오면 MERGE INTO 가 필요하고, 대시보드 일관성을 위해 Gold OVERWRITE 원자성이 필요했습니다. 거기에 Time-travel 로 audit 도 가능합니다.
셋째, 단일 AWS 환경. 앞 슬라이드에서 다뤘습니다.
넷째, DDL 은 Athena 에서 직접 실행합니다. Spark CREATE 가 아닙니다. Cold start 가 없고, `.sql` 파일이 git 으로 추적되며, 단일 진실원이 됩니다. 이게 Iceberg 네이티브 키 일부를 거부한다는 trade-off 는 32장 회고에서 다시 말씀드립니다.
다섯째, dbt 와 자동매매는 Phase 2 입니다. Phase 1 에서 욕심을 안 부린 결정입니다.

---

## 06. Iceberg 필요성 ① — DW / Data Lake 의 한계

> RDB·Parquet+Glue 만으론 시점성·갱신·스키마 진화 셋 다 잡기 어렵다.

- DW (RDBMS) : 대용량 append 와 분석 쿼리 비용·확장성 한계
- Plain Parquet + Glue : INSERT-only, 갱신 불가, snapshot 개념 없음
- 종목 마스터 갱신 (액면분할·상폐) → row 수정 필요
- BI 대시보드 일관성 → 부분 쓰기 노출 차단 필요

**시각자료**: 3열 비교 매트릭스 — RDB vs Parquet+Glue vs Iceberg / 갱신·시점성·스키마 진화·OVERWRITE 원자성.

**Speaker Note:**
왜 Iceberg 가 필요했냐는 질문에 두 슬라이드를 씁니다.
이 슬라이드는 "왜 다른 게 안 되나" 입니다.
RDB 는 분당 수만 건 tick 을 받아 분석 쿼리까지 같이 받기엔 비용이 안 맞습니다.
Plain Parquet + Glue 는 append 만 됩니다. 액면분할이 들어와서 종목 마스터의 par_value 를 수정해야 한다? row 수정이 불가능합니다.
또 Gold 분봉 집계를 OVERWRITE 할 때 부분 쓰기가 BI 에 노출되면 대시보드가 깜빡입니다. 이 일관성을 RDB 트랜잭션처럼 보장해줄 게 plain Parquet 에는 없습니다.
이 세 가지를 한 번에 푸는 게 Iceberg 의 ACID 트랜잭션과 snapshot 입니다.

---

## 07. Iceberg 필요성 ② — MERGE INTO / Time-travel 의 구체 시나리오

> 액면분할·OVERWRITE 원자성·Audit Trail — 셋 다 코드에서 실제로 쓰인다.

- 종목 마스터 MERGE : `silver_dim_symbol` 일일 MERGE (액면분할·상폐 반영)
- Gold OVERWRITE 원자성 : `gold_symbol_vwap_1m` 분봉 재집계가 BI 사용자에게 부분 노출 X
- Time-travel : `VERSION AS OF` 로 어제 분봉 재현 → audit / 정합성 검증
- Athena 가 Iceberg v2 만 지원 → row-level delete 가능

**시각자료**: 코드 한 컷 — `MERGE INTO silver_dim_symbol USING source ON …` 한 블록 + `SELECT * FROM gold VERSION AS OF <snapshot_id>` 한 블록.

**Speaker Note:**
구체적인 시나리오 세 가지입니다.
첫째, 종목 마스터 MERGE. dim_symbol_daily DAG 가 04:00 KST 에 KIS REST 종목 마스터를 받아 silver_dim_symbol 테이블에 MERGE 합니다. 액면분할이나 상폐가 들어오면 par_value, is_active 가 row 단위로 업데이트됩니다.
둘째, Gold OVERWRITE 원자성. silver_to_gold_vwap 이 10분마다 돌면서 분봉을 재집계합니다. 이 OVERWRITE 가 트랜잭션이 아니면 대시보드에 절반만 보이는 순간이 생깁니다. Iceberg 의 snapshot 교체로 이 순간이 사라집니다.
셋째, Time-travel. 어제 17시 분봉이 이상하다는 BI 사용자 리포트가 들어오면 VERSION AS OF 로 그 시점 스냅샷을 직접 조회해서 audit 합니다. plain Parquet 으로는 못 합니다.
이 셋이 코드에 실제로 들어가 있다는 점이 중요합니다. 추상적인 "있으면 좋은 기능" 이 아닙니다.

---

## 08. Bronze 스키마 — 왜 Parquet (Iceberg 아님)

> 13컬럼 Parquet + Glue Partition Projection — streaming snapshot 오버헤드 차단.

- 컬럼 : ingest_ts / kafka_partition·offset / symbol / trade_ts_kst / price / volume / cum_volume·amount / trade_side / best_ask·bid / raw_payload
- 파티션 : `dt date, hr int` (Glue Partition Projection — MSCK REPAIR 불필요)
- 1 minute micro-batch append-only, dedup 은 Silver 책임
- Iceberg 미선택 사유 : snapshot/manifest 갱신이 streaming 에서 순수 오버헤드

**시각자료**: DDL 한 컷 (kis_tick_raw.sql) — partition projection 부분 강조.

**Speaker Note:**
Bronze 는 13컬럼 Parquet 입니다.
원본 페이로드 raw_payload 와 함께 파싱된 12개 필드를 모두 들고 있어서 디버깅이 쉽습니다.
파티션은 dt 와 hr 두 개입니다. Glue Partition Projection 으로 메타데이터를 매번 갱신하지 않아도 Athena 가 알아서 경로를 만들어 줍니다. MSCK REPAIR 불필요합니다.
trigger 는 1분 micro-batch 입니다. 분당 한 번 S3 에 Parquet 을 떨굽니다.
dedup 은 Bronze 가 안 합니다. Bronze 는 원본 보존만 책임지고, 중복 제거는 Silver MERGE 에서 한 번에 처리합니다. 책임을 한 군데로 모아 두면 디버깅이 쉽습니다.
Iceberg 가 아닌 이유는 한 가지로 압축됩니다. 매분 snapshot 과 manifest 를 갱신해야 하는데 그게 순수 오버헤드입니다. Iceberg 가 주는 가치 (MERGE, Time-travel) 가 Bronze 에는 필요 없습니다.

---

## 09. Silver 스키마 — Iceberg COW 수용, MOR 전환 트리거 명시

> 12컬럼 Iceberg, hour partition, COW 기본값 수용 — write amplification 측정 시 MOR 전환.

- 컬럼 : trade_uid (dedup 키) / symbol / trade_ts_{kst,utc} / price / volume / trade_amount / side / best_ask·bid / ingest_ts / silver_ts
- 파티션 : `hour(trade_ts_kst)` (Iceberg 자체 표현식)
- DDL = Athena → `format-version=2`, `write.merge.mode=…` 등 일부 키 거부 → COW 기본값 사용
- MOR 전환 트리거 : MERGE 시 partition rewrite 비용 임계 초과 시 Spark bootstrap

**시각자료**: DDL 한 컷 (kis_tick_clean.sql) + COW vs MOR 비교 미니 표.

**Speaker Note:**
Silver 는 12컬럼 Iceberg 입니다.
파티션은 hour(trade_ts_kst) 로 Iceberg 의 hidden partitioning 을 씁니다. trade_ts_kst 가 들어오면 Iceberg 가 알아서 시간 파티션에 떨궈줍니다.
dedup 키는 trade_uid 입니다. Bronze 에서 kafka_partition + offset + symbol + trade_ts_kst 를 조합해 만든 결정적 키로, MERGE 의 ON 조건이 됩니다.
한 가지 trade-off 가 있습니다. DDL 을 Athena 에서 실행하다 보니 format-version=2 와 write.merge.mode 같은 Iceberg 네이티브 키 일부가 거부됩니다. 그래서 Phase 1 은 COW 기본값을 그대로 수용했습니다.
MERGE 시 partition rewrite 비용이 측정상 문제가 되면 Spark 로 ALTER TABLE 해서 MOR 로 전환할 계획입니다. 이건 100x 사고력 슬라이드에서 다시 다룹니다.

---

## 10. Gold 스키마 — OVERWRITE + 1분봉 일괄집계

> 10컬럼 Iceberg, OVERWRITE 원자성 — 증분집계 X, 일괄집계 O.

- 컬럼 : symbol / ts_minute / open·close·high·low_price / total_volume / vwap / trade_count / computed_at
- 파티션 : `hour(ts_minute)` (Iceberg)
- 집계 : Silver 에서 GROUP BY symbol, minute → VWAP / OHLC 일괄 OVERWRITE (마지막 N 시간)
- 증분 안 한 이유 : 분봉이 늦게 도착한 tick 으로 재집계되어야 함 → 일괄이 단순하고 정확

**시각자료**: DDL + 집계 SQL 한 컷 — `INSERT OVERWRITE … GROUP BY symbol, date_trunc('minute', trade_ts_kst)`

**Speaker Note:**
Gold 는 10컬럼 Iceberg 분봉입니다.
한 분 단위로 symbol 별 OHLC, VWAP, 거래량, 거래대금, tick 카운트를 집계합니다.
중요한 결정 한 가지가 있습니다. 증분집계가 아니라 일괄집계입니다. 마지막 N 시간 윈도우를 통째로 다시 계산해서 OVERWRITE 합니다.
이유는 두 가지인데, 첫째, 늦게 도착한 tick 이 있으면 그 분의 VWAP 가 다시 계산되어야 정확합니다. 둘째, 증분이 더 복잡하고 디버깅이 어렵습니다. 분당 데이터 규모가 작아서 일괄을 감당할 수 있다는 게 일괄을 고른 핵심 근거입니다.
Phase 2 에서 일 1억 tick 규모가 되면 이 결정을 다시 봐야 합니다. 윈도우 증분 OVERWRITE 로 갈 수도 있고, Flink streaming 으로 갈 수도 있습니다.

---

## 11. Kafka topic·파티션 설계

> `kis.tick.raw` 1 topic, partition key = symbol, 12 partitions, retention 7일.

- Topic : `kis.tick.raw` (단일) — Bronze 외 다른 source 분리 시 별도 topic
- Partitions : 12 — symbol 분포 + Spark Streaming 병렬도 균형
- Partition key : symbol → 동일 symbol 순서 보장
- Metadata 보존 : `kafka_partition`·`kafka_offset` 컬럼 Bronze 에 저장 → 장애 시 추적

**시각자료**: 토픽 설계도 — KIS Producer → Kafka topic (12 partitions) → Spark Streaming consumer.

**Speaker Note:**
Kafka 설계는 단순합니다.
Topic 은 kis.tick.raw 하나입니다. DART 나 신용정보원은 Kafka 를 안 거치고 직접 S3 에 떨굽니다. 일배치라 Kafka 가 필요 없습니다.
파티션은 12 개입니다. KOSPI200 종목 분포 + Spark Streaming consumer 병렬도 (executor 수) 두 가지를 보고 정했습니다.
partition key 는 symbol 입니다. 같은 종목의 tick 은 항상 같은 파티션으로 가서 순서가 보장됩니다. VWAP 계산할 때 시간 순서가 어긋나면 OHLC 가 깨집니다.
한 가지 강조하고 싶은 건 metadata 까지 Bronze 에 저장한다는 점입니다. kafka_partition 과 kafka_offset 을 컬럼으로 남겨 두면 "이 row 가 어느 파티션 어느 오프셋에서 왔는지" 가 그대로 추적됩니다. 장애 분석에 매우 유용합니다.

---

## 12. Kafka 튜닝값 — 운영 시 고민한 4가지

> acks=all + idempotence / linger.ms=50 / compression=snappy / metadata_max_age=30s — 손실·중복·지연·장애 균형.

- `acks=all` + `enable_idempotence=True` : 메시지 손실 + 재시도 중복 동시 차단 (exactly-once 지향)
- `linger.ms=50` : 50ms 묶음 전송 → throughput 와 지연 균형
- `compression.type=snappy` : 네트워크·디스크 절감, CPU 부하 낮음
- `metadata_max_age_ms=30000` : broker 재시작 후 stale metadata 윈도우 단축 (기본 5분→30초) — 5/14 운영 중 Kafka 재시작 → producer stuck 사고 후 보강

```python
# infra/docker/kis-producer/main.py
producer = AIOKafkaProducer(
    bootstrap_servers=bootstrap,
    acks="all", enable_idempotence=True,
    linger_ms=50, compression_type="snappy",
    request_timeout_ms=30000, retry_backoff_ms=500,
    metadata_max_age_ms=30000,   # stale metadata 윈도우 ↓
)
```

**시각자료**: 4행 표 — 키 / 값 / 근거 / Trade-off + 위 코드 한 컷.

**Speaker Note:**
Producer 튜닝값 4가지입니다. aiokafka 기준입니다.
첫째, acks=all 과 enable_idempotence 를 같이 켰습니다. acks=all 은 메시지 손실을 막고, idempotence 는 재시도 시 중복을 막습니다. 둘을 같이 켜야 exactly-once 에 가까워집니다. idempotence 가 켜지면 aiokafka 가 retries 를 내부적으로 알아서 잡으므로 retries 를 명시하지 않습니다.
둘째, linger.ms 는 50ms 입니다. 0 으로 두면 메시지가 들어오자마자 보내서 throughput 이 떨어지고, 너무 길게 두면 지연이 늘어납니다. 50ms 가 분당 tick 수 기준으로 균형점이었습니다.
셋째, compression 은 snappy 입니다. CPU 부하가 낮으면서 네트워크와 S3 비용을 둘 다 줄여줍니다.
넷째, metadata_max_age_ms 는 30초입니다. 이건 운영 중 실제 사고 후 추가한 값입니다. 5/14 발표 준비 중 Kafka 컨테이너가 재시작됐는데, producer 가 stale metadata 를 들고 무한 재시도에 빠졌습니다. 기본값 5분이던 metadata 갱신 주기를 30초로 줄여 그 윈도우를 좁혔습니다. 근본 해결은 아니라서 — aiokafka 내부 reconnect 한계 — self-healing guard 를 Phase 2 로 잡았고, 자세한 건 트러블슈팅 문서에 남겼습니다.
RF 가 1 이라 acks=all 이 사실상 단일 리더 응답이지만, Phase 2 에서 RF=3 으로 갈 때 코드 변경 없이 안전성이 올라갑니다.

---

## 13. Airflow DAG 설계 — 장 시간 vs 장 마감 후 분리

> 6 개 DAG, 시간대 분리 — 장 시간 = 매시 트리거, 장 마감 후·주말 = 매니지먼트.

- 장 시간 (09:30–15:30 KST 평일) : `bronze_to_silver_kis` / `silver_to_gold_vwap` (`30 9-15`)
- 장 시작 전 : `dim_symbol_daily` 04:00 / `dart_ingest_daily` 06:00 평일
- 장 마감 후 (평일 18:00) : `iceberg_compaction` (Silver/Gold rewrite_data_files)
- 주 1회 (일요일 19:00) : `expire_snapshots` (Silver/Gold snapshot 정리)

**시각자료**: DAG 그래프 한 컷 — 시간 축 가로, DAG 6개 세로 배치.

**Speaker Note:**
DAG 는 6개입니다. 핵심 원칙은 장 시간 자원과 장 마감 후·주말 자원을 분리하는 것입니다.
장 시간에는 두 DAG 가 09:30부터 매시 30분에 돕니다. 마지막 run 이 정규장 마감인 15:30 에 정확히 맞춰져서, 장 마감 후엔 빈 배치 run 이 한 번도 안 돕니다. Bronze 를 Silver 로 MERGE 하는 DAG 와 Silver 를 Gold 분봉으로 OVERWRITE 하는 DAG 입니다.
간격은 처음 설계가 아니라 운영 중 조정한 값입니다. 처음엔 10분으로 뒀는데, Bronze 가 1분 micro-batch 라 small-files 가 누적되면서 bronze→silver MERGE 가 20분 가까이 걸리기 시작했습니다. 10분 스케줄에 20분 job 이면 run 이 영구 적체됩니다. 그래서 (1) Spark executor core 를 줄여 두 batch DAG 가 동시 실행 가능하게 하고 (2) 스케줄을 매시 1회로 늘려 job 소요시간보다 길게 잡고 정규장 시간에 정렬했습니다. 근본 원인인 Bronze small-files 압축은 Phase 2 입니다. 이 진단·조정 과정은 트러블슈팅 문서에 남겼습니다.
장 시작 전 두 DAG 가 있습니다. 04:00 에 KIS REST 종목 마스터를 MERGE 하고, 06:00 평일에 전일 DART 공시를 일배치로 끌어옵니다. 장 시간 자원과 충돌하지 않습니다.
장 마감 후 평일 18:00 에 Iceberg Compaction 이 돕니다. rewrite_data_files 로 Silver 와 Gold 의 작은 파일을 384MB 단위로 합칩니다.
주말에는 일요일 19:00 에 expire_snapshots 가 돕니다. 30일 이전 snapshot 을 정리합니다. Compaction 과 Expire 는 짝입니다.
Spark cluster 가 가장 한가한 시간대를 골라서 매니지먼트를 배치한 게 핵심입니다.

---

## 14. 로그 설계 — 무엇을 남겼나

> Airflow log + Spark task log + 비즈니스 카운트 — 5분 안에 어느 단계가 깨졌는지 보인다.

- Airflow task_instance : DAG / task / try_number / log_url / 실행 시간
- Spark task 로그 : Iceberg snapshot id, rewrite metrics, output rows
- 비즈니스 카운트 : Bronze rows → Silver rows → Gold rows (각 단계 로그)
- 의도 : 평가자가 Airflow UI 한 화면에서 단계별 row 수 차이를 즉시 확인

**시각자료**: 로그 샘플 컷 — Spark stdout 의 "wrote N rows to silver_kis_tick_clean" 라인 한 줄 + Airflow task UI 스크린샷 placeholder.

**Speaker Note:**
로그는 세 층입니다.
첫째, Airflow task_instance. DAG 이름, task 이름, try 번호, 실행 시간, 로그 URL 이 자동으로 기록됩니다. UI 에서 바로 확인 가능합니다.
둘째, Spark task 로그. Iceberg snapshot id, rewrite metrics, 그리고 output row 수를 남깁니다. snapshot id 가 로그에 남아 있어서 나중에 audit 할 때 Time-travel 의 기준점이 됩니다.
셋째, 비즈니스 카운트. Bronze, Silver, Gold 각 단계에서 처리한 row 수를 로그에 찍습니다. 단계 사이 비율이 깨지면 어느 단계가 문제인지 1초에 보입니다.
의도는 5분 헬스체크입니다. 평가자가 운영자 입장으로 본다고 가정하고, Airflow UI 한 화면만 봐도 어느 단계가 깨졌는지 보이도록 설계했습니다.

---

## 15. 검증 쿼리 4종

> Athena 한 줄 쿼리로 Bronze freshness / Silver dedup / symbol coverage / Gold partition completeness.

- `01_bronze_freshness.sql` : `lag_minutes` (영업시간 1–2 분 정상)
- `02_silver_dedup_rate.sql` : `silver_rows / bronze_rows` (0.95–1.0 정상)
- `03_symbol_coverage.sql` : 최근 15분 종목 카운트 (3 종목 모두)
- `04_gold_partition_completeness.sql` : hour 별 row 수 (180 = 3 × 60 정상)

**시각자료**: 4개 쿼리 결과 카드 — 각 쿼리 한 줄 결과를 카드 4장으로.

**Speaker Note:**
검증 쿼리는 4종입니다. 모두 Athena 한 번에 도는 짧은 쿼리입니다.
첫째, Bronze freshness. 가장 최근 ingest_ts 와 현재 시각 사이 분 차이입니다. 영업시간이면 1–2분, 장 마감 후엔 자연스럽게 커집니다.
둘째, Silver dedup rate. Bronze row 수와 Silver row 수를 비교한 비율입니다. 0.95에서 1.0 사이가 정상이고, 그 미만이면 MERGE 가 과도하게 dedup 했거나 누락된 것입니다.
셋째, symbol coverage. 최근 15분 동안 3 종목이 모두 나타나는지 봅니다. 한 종목이 빠지면 KIS WebSocket subscriber 가 그 종목만 끊겼을 가능성이 있습니다.
넷째, Gold partition completeness. 시간당 row 수가 3 종목 × 60 분 = 180 이어야 합니다. 빠지면 분봉 결손이 생긴 거고 즉시 추적합니다.

---

## 16. 데이터 퀄리티 체크

> 4 종 헬스 쿼리 + 비즈니스 카운트 로그 → NULL·중복·시점 일관성 모두 잡힘.

- NULL : Silver DDL 의 `trade_uid` 필수 / `price>0` 필터
- 중복 : Silver MERGE 의 `ON trade_uid` (멱등성 보장)
- 시점 일관성 : `ingest_ts` vs `trade_ts_kst` 차이 모니터링 → 비정상 lag 감지
- 향후 : Great Expectations / Soda 도입 검토 (Phase 2)

**시각자료**: 체크 매트릭스 — 차원(NULL/중복/시점) × 방어선(DDL/MERGE/쿼리/향후).

**Speaker Note:**
퀄리티 체크는 세 차원입니다.
NULL 차원. Silver DDL 에 trade_uid 가 필수 필드라 NULL 이면 INSERT 자체가 실패합니다. price 가 0 인 비정상 tick 은 Silver 변환 시 필터로 제거합니다.
중복 차원. Silver MERGE 의 ON 조건이 trade_uid 입니다. 같은 trade_uid 가 두 번 들어와도 update 됩니다. 즉, MERGE 가 멱등입니다.
시점 일관성. ingest_ts 와 trade_ts_kst 차이를 모니터링합니다. WebSocket → Kafka → Bronze 까지의 lag 가 비정상이면 즉시 잡힙니다.
Phase 2 에서는 Great Expectations 나 Soda 같은 데이터 퀄리티 프레임워크를 검토할 예정입니다. Phase 1 은 헬스 쿼리 4 종 + 비즈니스 카운트 로그로 충분히 잡힌다고 봤습니다.

---

## 17. Superset Dashboard ① — 비즈니스 KPI 탭

> VWAP 추세 / 분봉 거래량 / 거래량 점유율 — 3 chart, Gold 테이블 직접 조회.

- VWAP 추세 (Line) : `gold_symbol_vwap_1m` JOIN `silver_dim_symbol`, 종목별 1분봉 VWAP 시계열
- 분봉 거래량 (Bar) : 종목별 분당 `total_volume`
- 종목별 거래량 점유율 (Pie) : 영업일 1일 누적 거래량 점유율
- 데이터 출처 : `tickberg.gold_symbol_vwap_1m` (Athena via Superset)

**시각자료**: KPI 탭 스크린샷 placeholder — VWAP 라인 + 거래량 바 + 점유율 파이 3 chart. `<TODO: 실측 스샷 추가>`

**Speaker Note:**
비즈니스 KPI 탭은 세 차트로 구성했습니다.
VWAP 추세는 종목별 1분봉 VWAP 의 시계열입니다. gold 테이블과 종목 마스터를 JOIN 해서 종목명으로 보여줍니다. 거래 흐름을 한 눈에 봅니다.
분봉 거래량은 종목별 분당 거래량 막대 차트입니다. 어느 종목이 지금 가장 활발한지 보입니다.
종목별 거래량 점유율은 영업일 1일 누적 기준 파이 차트입니다.
데이터는 모두 Gold 테이블 한 곳에서 옵니다. 비즈니스 정의 변경 요청이 들어와도 Gold DDL 과 집계 SQL 두 군데만 보면 됩니다.
실제 스크린샷은 발표 직전 캡처해서 슬라이드에 박아 넣을 예정입니다.

---

## 18. Superset Dashboard ② — 운영 Data Quality 탭

> Bronze freshness / Symbol coverage / Silver throughput / DART 타임라인 — 운영자 5분 헬스체크.

- Bronze Freshness (Big Number) : `MAX(ingest_ts)` 경과 분, 노랑 5분·빨강 10분 (장 시간 한정)
- Symbol Coverage Bar (최근 1h) : 3 종목 막대, 누락 즉시 식별
- Silver Throughput per 5min (24h) : 영업시간 패턴 시각화 (09:00 급상승 / 15:30 종료)
- DART 공시 타임라인 (Table) : 최근 14일 공시, VWAP·거래량과 cross-reference

**시각자료**: 운영 탭 스크린샷 placeholder — 4 chart grid. `<TODO: 실측 스샷 추가>`

**Speaker Note:**
운영 탭은 평가 4축의 운영 가시성에 정면으로 답하는 슬라이드입니다.
Bronze Freshness 는 Big Number 위젯입니다. 마지막 적재 후 경과 분을 보여주고, 장 시간대 기준 5분이면 노랑, 10분이면 빨강으로 바뀝니다. 장 마감 후엔 자연스럽게 커지니 임계는 장 시간대 한정입니다.
Symbol Coverage Bar 는 최근 1시간 종목별 tick 수 막대입니다. 3 종목 중 하나가 빠지면 즉시 보입니다.
Silver Throughput per 5min 은 24시간 처리량 추이입니다. 09:00 급상승하고 15:30 에 종료되는 영업시간 패턴이 그대로 그려집니다.
DART 공시 타임라인은 최근 14일 공시 테이블입니다. 공시가 VWAP 와 거래량에 어떻게 반영되는지 발표 중에 cross-reference 합니다.
Grafana 와 역할을 나눴습니다. Grafana 는 시스템 헬스, Superset 은 데이터 품질과 비즈니스 KPI 입니다. Iceberg snapshot 추이 차트는 시간 여유 시 추가할 예정입니다.

---

## 19. 운영 ① — Snapshot / Time-travel 활용

> Iceberg snapshot 마다 BI 일관성 + audit 두 가치 — 어제 분봉 재현 가능.

- 모든 Silver/Gold 쓰기 = 새로운 snapshot 생성
- 사용 케이스 1 : BI 사용자 "어제 17시 분봉 이상" → `VERSION AS OF` 로 재현
- 사용 케이스 2 : Compaction 결과 검증 → 전후 snapshot 비교
- Athena : `SELECT * FROM …$snapshots` 로 snapshot 메타 직접 조회

**시각자료**: Iceberg snapshot 타임라인 — Silver MERGE / Gold OVERWRITE / Compaction 각 스냅샷이 시간축에 점으로.

**Speaker Note:**
Iceberg 의 snapshot 은 두 가치를 동시에 줍니다.
첫째, BI 일관성. 모든 쓰기가 새 snapshot 을 만들고, 읽기는 가장 최신 snapshot 만 봅니다. 그래서 OVERWRITE 중간 상태가 BI 에 노출되지 않습니다.
둘째, audit. BI 사용자가 어제 17 시 분봉이 이상하다고 리포트하면 그 시점 snapshot 을 VERSION AS OF 로 직접 조회합니다. 코드 한 줄로 됩니다.
Athena 는 `…$snapshots` 라는 메타 테이블을 자동으로 노출합니다. snapshot id, parent_id, committed_at, summary 가 그대로 조회됩니다.
헬스 쿼리 6번이 바로 이 메타 테이블로 snapshot 누적을 모니터링합니다.
Compaction 도 이 메커니즘 위에서 안전해집니다. 새 snapshot 으로 rewrite 된 결과를 만들고 atomic 하게 교체합니다.

---

## 20. 운영 ② — Rollback 활용 원칙

> "Rollback 이 자주 일어난다면 파이프라인 설계가 잘못된 건 아닐지 의심해보자."

- 절차 : Athena `CALL iceberg.system.rollback_to_snapshot(<table>, <snapshot_id>)`
- 사용 케이스 : Gold OVERWRITE 직후 데이터 결손 발견 → 직전 snapshot 으로 rollback
- 원칙 : Rollback 은 emergency 도구, 일상 도구가 아님
- 잦은 rollback = 사전 검증 부재 → 검증 쿼리·DQ 체크 강화로 대응

**시각자료**: rollback 절차 다이어그램 + 경고 박스 ("자주 일어나면 설계 의심").

**Speaker Note:**
Rollback 슬라이드는 두 메시지입니다.
첫째, 실제 절차. Athena 에서 system.rollback_to_snapshot 프로시저를 호출합니다. 한 줄입니다.
둘째, 이게 핵심인데, Rollback 이 자주 일어난다면 파이프라인 설계가 잘못된 건 아닐지 의심해야 합니다.
Rollback 은 emergency 도구입니다. 일상적으로 의존하면 그건 사전 검증이 부족하다는 신호입니다.
그래서 저는 Rollback 을 만들어 두되, 검증 쿼리 4 종과 데이터 퀄리티 체크를 강화해서 Rollback 이 필요한 상황 자체를 줄이는 데 집중했습니다.
지금까지 Phase 1 운영 중 Rollback 호출 횟수는 <TODO: 실측 횟수> 입니다.

---

## 21. 운영 ③ — Expire Snapshots 정책

> 30일 보존 + 최소 5 snapshot 유지 — 주 1회 일요일 19:00 KST, Iceberg 자동화 #2.

- 정책 : `expire_snapshots(older_than=30d, retain_last=5)` — Iceberg 가 보수적인 쪽 적용
- 스케줄 : 일요일 19:00 KST (`0 19 * * SUN`) — 장 마감 후 retention window
- 대상 : silver_kis_tick_clean / silver_dart_disclosure_clean / gold_symbol_vwap_1m
- Compaction 과 짝 : file 병합(평일 18:00) + metadata 정리(일요일 19:00)

**시각자료**: 정책 표 — 파라미터 / 값 / 근거 + Compaction–Expire 짝 타임라인.

**Speaker Note:**
Expire Snapshots 는 Iceberg 자동화의 두 번째입니다. 첫 번째 Compaction 과 짝을 이룹니다.
정책은 30일 이전 snapshot 을 정리하되 최소 5개는 무조건 유지합니다. Iceberg 가 older_than 과 retain_last 중 보수적인 쪽을 적용해서, retention 이 짧아도 최근 5개는 절대 안 지웁니다. Rollback 과 Time-travel 윈도우를 보장하기 위해서입니다.
스케줄은 주 1회 일요일 19:00 KST 입니다. 평일이 아닌 이유는 snapshot 정리가 자주 필요한 작업이 아니고, 주말 장 마감 시간대가 Spark 리소스가 가장 한가하기 때문입니다.
대상은 Iceberg 테이블 셋입니다. silver_kis_tick_clean, silver_dart_disclosure_clean, gold_symbol_vwap_1m.
Compaction 은 평일 18:00 에 파일을 병합하고, Expire 는 일요일 19:00 에 메타데이터를 정리합니다. 둘이 합쳐져야 storage 와 query plan 이 둘 다 최적화됩니다.
한 테이블 expire 가 실패해도 다음 테이블로 넘어가도록 WARN 처리해서, 한 번의 실패가 전체를 막지 않습니다.

---

## 22. 운영 ④ — Remove Orphan + Compaction (쿼리 플랜)

> Compaction target 384MB, band 256–512MB — 쿼리 플랜의 file scan 수 감소가 측정 가능.

- Compaction : `rewrite_data_files(target=384MB, min=256MB, max=512MB)`
- Min-files 변수 분리 : 하드코딩 X, 함수 인자로 (`code/pipelines/silver/iceberg_compaction.py`)
- Orphan files : 18:00 KST 평일 Compaction 후 별도 cleanup 검토 (Phase 1.5)
- 효과 : 쿼리당 file scan 수 N → N/M 감소 → Athena 비용·지연 모두 감소

**시각자료**: before/after 비교 — file count 100 → 12, scan time 변화. `<TODO: 실측값>`

**Speaker Note:**
Compaction 은 Iceberg 매니지먼트의 핵심입니다.
설정은 target 384MB, min 256MB, max 512MB 입니다. 너무 작은 파일은 검색이 비효율적이고, 너무 큰 파일은 partial scan 이 어렵습니다. 그 사이 sweet spot 입니다.
중요한 코드 컨벤션 하나가 min/max/target 을 함수 인자로 받는다는 점입니다. 하드코딩하지 않고 변수로 분리해서, Silver 와 Gold 가 같은 함수에 다른 값을 줄 수 있습니다.
Orphan files cleanup 은 Phase 1.5 로 미뤘습니다. 운영 초기에 orphan 이 거의 안 쌓이는 게 측정되었기 때문입니다.
효과는 쿼리 플랜에서 측정됩니다. file scan 수가 줄면 Athena 비용과 지연이 둘 다 떨어집니다. 실측값은 발표 직전 캡처합니다.

---

## 23. 운영 ⑤ — 테이블별 정책 매트릭스

> 모든 테이블에 같은 정책 X — 데이터 성격별로 Compaction·Expire·Lifecycle 분리.

- Bronze (Parquet) : S3 Lifecycle 90일 → Glacier IR / athena-results 30일 만료
- Silver tick·DART (Iceberg) : Compaction 평일 18:00 / Expire 일요일 19:00 (30d, retain 5)
- Gold vwap_1m (Iceberg) : Compaction 평일 18:00 / Expire 일요일 19:00 (30d, retain 5)
- dim_symbol (Iceberg) : 일 1회 MERGE 만 — row 수 적어 Compaction·Expire 대상 제외

**시각자료**: 매트릭스 — 테이블 4행 × 정책 열 (Compaction / Expire / Lifecycle / 비고).

**Speaker Note:**
테이블별로 정책이 다른 이유는 데이터 성격이 다르기 때문입니다.
Bronze 는 Parquet 이라 Iceberg 매니지먼트가 없습니다. 대신 S3 Lifecycle 로 90일 후 Glacier IR 로 보냅니다. KIS API 재호출 비용보다 Glacier IR 보관 비용이 훨씬 싸서 이 결정이 맞습니다. Athena 쿼리 결과도 30일 후 자동 만료시킵니다.
Silver 의 tick 테이블과 DART 테이블, 그리고 Gold 분봉 테이블은 동일한 Iceberg 정책을 받습니다. 평일 18:00 Compaction, 일요일 19:00 Expire, 30일 보존에 최소 5 snapshot 유지입니다.
Compaction 은 파일을 합치고 Expire 는 메타데이터를 정리합니다. 둘은 짝입니다.
dim_symbol 은 다릅니다. 일 1회 MERGE 만 도는 종목 마스터라 row 수가 수천 건 수준입니다. 작은 파일 문제도 snapshot 누적 문제도 거의 없어서 Compaction 과 Expire 대상에서 아예 제외했습니다. 불필요한 Spark job 을 안 돌리는 게 비용 측면에서 맞습니다.
이 매트릭스가 의미하는 건 정책을 통합하지 않고 의도적으로 분리했다는 점입니다. 6개월 후 합류한 팀원이 이 표를 보면 왜 다르게 운영되는지 한 번에 이해할 수 있습니다.

---

## 24. 운영 ⑥ — 동시성 충돌 회피 패턴

> Streaming·MERGE·Compaction·Expire 가 같은 테이블을 건드려도 conflict 가 안 나는 이유.

- Iceberg snapshot isolation : 각 writer 가 base snapshot 기반 새 snapshot atomic 생성
- 시간대 분리 : MERGE/OVERWRITE 평일 09–16 / Compaction 평일 18:00 / Expire 일요일 19:00
- `max_active_runs=1` : 같은 DAG 의 run 이 절대 겹치지 않음 (연속 MERGE 중첩 차단)
- DAG `retries=2` (retry_delay 1분) : 일시 충돌·장애 시 Airflow 레벨 자동 재시도

**시각자료**: 동시성 타임라인 — 09–16 MERGE/OVERWRITE / 18:00 Compaction / 일요일 19:00 Expire 시간축 분리.

**Speaker Note:**
Iceberg 가 ACID 트랜잭션을 주지만 그게 자동으로 모든 충돌을 막아주는 건 아닙니다.
충돌은 두 writer 가 같은 테이블의 같은 파티션을 동시에 rewrite 할 때 발생합니다.
첫 번째 회피 패턴은 시간대 분리입니다. MERGE 와 OVERWRITE 는 평일 09시에서 16시, Compaction 은 평일 18시, Expire 는 일요일 19시입니다. 매니지먼트 작업과 적재 작업이 시간상 절대 겹치지 않습니다.
두 번째는 max_active_runs 를 1로 둔 겁니다. 한 DAG 의 이전 run 이 안 끝났는데 다음 스케줄이 와도 새 run 이 시작되지 않습니다. 연속된 Silver MERGE 가 중첩되는 상황 자체가 없습니다.
세 번째는 Airflow DAG 레벨 retry 입니다. retries 2, retry_delay 1분. 일시적인 충돌이나 장애가 나도 1분 뒤 자동으로 재시도합니다.
이 세 가지면 Phase 1 규모에서 conflict 로 인한 운영 사고는 사실상 0 입니다.

---

## 25. 매트릭 모니터링 — Prometheus + Grafana

> KIS Producer / Kafka JMX / Spark master·driver → Prometheus 15s scrape → Grafana.

- Scrape 대상 4종 : kis-producer:9100 / kafka-jmx-exporter:7071 / spark-master / spark-driver
- KIS Producer 메트릭 : `kis_ws_connected` / `kis_parse_errors_total` / `kis_token_refresh_failures_total` / `kis_messages_published_total`
- Alert 6종 : WebSocket Down / Parse error spike·burst / Token refresh fail·trend / Broker rate mismatch
- Grafana 대시보드 : `tickberg-1a` `tickberg-1b` — 장애 인지 5분 이내 목표

**시각자료**: Grafana 대시보드 스크린샷 placeholder + Prometheus scrape 토폴로지. `<TODO: 실측 스샷>`

**Speaker Note:**
모니터링은 Prometheus 와 Grafana 입니다. 15초 간격으로 scrape 합니다.
Scrape 대상은 네 종류입니다. KIS Producer, Kafka JMX exporter, Spark master, Spark driver. 각자 Prometheus 메트릭 엔드포인트를 노출합니다.
KIS Producer 가 가장 중요한 메트릭을 냅니다. WebSocket 연결 상태, parse error 수, 토큰 갱신 실패 수, 발행한 메시지 수입니다.
Alert 는 6종입니다. WebSocket 이 5분간 끊기면 critical, parse error 가 튀면 warning, 토큰 갱신이 실패하면 critical 같은 식입니다.
한 가지 강조하고 싶은 건 WebSocket Down alert 의 description 에 "장 마감 후엔 무시 가능" 이라고 명시했다는 점입니다. 장 시간과 장 마감 후를 alert 단에서 구분합니다.
Kafka 쪽은 JMX exporter 로 broker in-rate 를 받아서, producer 가 publish 하는 rate 와 broker 가 받는 rate 의 차이가 분당 500을 넘으면 alert 합니다.
Grafana 대시보드는 1a, 1b 두 개입니다. 목표는 장애 인지 시간 5분 이내입니다.

---

## 26. 예상 문제 ① — Small Files / Scale out 10x·100x

> 일 100만 → 1억 tick 시 첫 번째로 깨지는 건 Bronze 1분 trigger.

- Small files : Bronze 분당 micro-batch → S3 Parquet 수 = 분당 1 + 종목수 × 파티션
- 10x (일 1천만) : Spark micro-batch trigger 1 → 5 분으로 늘림 / Compaction 빈도 증가
- 100x (일 1억) : Bronze = MSK / EMR Serverless / Spark Structured Streaming → Flink 전환 검토
- Silver write amplification (COW) : MERGE 시 partition rewrite 비용 → MOR 전환

**시각자료**: 깨지는 순서 다이어그램 — 1x → 10x → 100x 단계별 첫 번째 깨지는 자원.

**Speaker Note:**
이 슬라이드는 평가 4 축 중 100x 사고력에 정면으로 답합니다.
일 100 만 tick 인 현재 규모에서 가장 먼저 깨지는 건 Bronze 1 분 micro-batch 입니다. 분당 파일 수가 너무 많아지면 S3 LIST 비용이 폭증합니다. 10 배 규모면 trigger 를 5 분으로 늘리고 Compaction 빈도를 키우는 게 합리적입니다.
100 배 규모면 Bronze 를 MSK + EMR Serverless 로 옮기는 걸 검토합니다. Spark Structured Streaming 의 한계를 넘으면 Flink 전환도 고려합니다.
Silver 의 COW write amplification 도 100x 에서 깨집니다. MERGE 시 partition 통째로 rewrite 하는데, partition 당 데이터 양이 크면 비용이 폭증합니다. MOR 로 전환해야 합니다.
중요한 건 "지금 깨진 게 아니고, 깨질 순서를 사전에 알고 있다" 는 점입니다.

---

## 27. 예상 문제 ② — OOM / S3 / Kafka 장애 (5/14 실제 발생 포함)

> 어디서 데이터가 유실 가능한지 정확히 안다 — Kafka 이후는 replay, Kafka 이전은 알람.

- Spark Bronze consumer OOM : Kafka offset checkpoint (S3) → 재기동 시 마지막 offset 부터 재개 (유실 0)
- KIS Producer OOM/stuck : 라이브 WebSocket 구간이라 다운타임 tick 유실 → `restart=unless-stopped` + `kis_ws_connected` 알람. **단 stuck-loop (프로세스 안 죽음) 은 restart 정책 미발동** → self-healing guard 는 Phase 2
- S3 PUT 일시 장애 : Spark Structured Streaming checkpoint 가 미완료 batch 다음 trigger 에 재시도
- **5/14 실제 발생** : Kafka 컨테이너 재시작 → KRaft controller stale 등록으로 재시작 루프 + aiokafka producer stuck. 위 대응 체계로 5분 내 인지·복구, 6건 트러블슈팅 문서화
- 가드레일 : Kafka retention 7일 = consumer replay 윈도우 / AWS Budgets 월 $20 알람

```python
# bronze_kis_tick_streaming.py — Kafka offset 이 S3 checkpoint 에 기록
# → Spark consumer 가 죽어도 마지막 offset 부터 재개, 유실 0
CHECKPOINT_PATH = f"s3a://{BUCKET}/checkpoints/bronze_kis_tick/"
```

**시각자료**: failure mode 표 — 장애 지점 / 영향 / 복구 / 안전망 (Kafka 전후 구분) + 5/14 실제 사례 칸.

**Speaker Note:**
운영 중 장애를 다룰 때 핵심은 어디서 데이터가 유실될 수 있는지를 정확히 아는 것입니다.
Kafka 이후 구간은 안전합니다. Spark Bronze consumer 가 OOM 으로 죽어도 Kafka offset checkpoint 가 S3 에 있어서, 재기동하면 마지막 offset 부터 재개합니다. 유실이 0 입니다. 일부 메시지가 중복돼도 Silver MERGE 의 trade_uid 가 멱등이라 안전합니다.
Kafka 이전 구간은 다릅니다. KIS Producer 가 죽으면 KIS WebSocket 은 라이브 피드라 그 다운타임 동안의 tick 은 유실됩니다. 솔직히 인정하는 한계입니다. 대응은 restart=unless-stopped 자동 재기동 + kis_ws_connected 메트릭 5분 알람입니다. 다만 한 가지 정직하게 짚으면 — 프로세스가 *죽지 않고* stuck-loop 만 도는 경우엔 restart 정책이 발동하지 않습니다. 이건 self-healing guard 로 Phase 2 에서 보강합니다.
이 슬라이드가 "예상 문제" 인데, 사실 5/14 발표 준비 중에 정확히 이 시나리오를 실제로 겪었습니다. Kafka 컨테이너가 재시작됐고, KRaft controller 가 이전 broker 등록을 안 놔줘서 재시작 루프에 빠졌고, 그 사이 aiokafka producer 가 stuck 됐습니다. 중요한 건 — 위에 설계해 둔 대응 체계가 그대로 작동했다는 점입니다. ws_connected 알람으로 5분 안에 인지했고, 명시적 stop·start 와 producer 재시작으로 복구했습니다. 그리고 이 6건을 전부 트러블슈팅 문서로 남겼습니다. 장애 대응이 슬라이드 위의 가설이 아니라 실제로 한 번 돌아간 체계라는 게 핵심입니다.
S3 PUT 이 일시적으로 장애가 나도 Spark Structured Streaming 의 checkpoint 가 미완료 batch 를 다음 trigger 에서 재시도합니다.
Kafka retention 을 7일로 둔 이유가 여기 있습니다. consumer replay 윈도우입니다. 월요일 아침에 발견한 이슈를 금요일까지 거슬러 재처리할 수 있습니다.
비용 가드레일도 별도입니다. AWS Budgets 로 월 20달러 알람이 걸려 있습니다.

---

## 28. 코드·인프라 노력 — 강결합 회피, 분리, 자동화

> 6 개월 후 합류한 팀원이 합류 가능하도록 — DDL·변수·비용·매니지먼트 모두 분리.

- DDL·struct 분리 : `code/ddl/*.sql` 단일 진실원, 스키마 변경 = git diff
- Compaction·Expire 변수 분리 : target/min/max·retention 하드코딩 X → 함수 인자
- 비용·권한 가드 : Athena workgroup 5GB scan cutoff / IAM 최소 권한 / Terraform 자동화 (Phase 1.5)
- 장 마감 후 매니지먼트 : Compaction 평일 18:00 + Expire 일요일 19:00 — 적재와 시간 분리

**시각자료**: 표 — 노력 / 어디에 / 효과.

**Speaker Note:**
평가 4 축 중 협업·지속가능성 축에 정면으로 답하는 슬라이드입니다.
DDL 은 .sql 파일이 단일 진실원입니다. 스키마 변경은 git diff 로 추적되고, code review 의 대상이 됩니다.
Compaction 의 target 384MB, Expire 의 retention 30일 같은 숫자가 코드 안에 하드코딩되어 있지 않습니다. 함수 인자로 분리되어서 테이블마다 다른 값을 줄 수 있고, 변경할 때 한 곳만 보면 됩니다.
비용과 권한은 가드를 걸었습니다. Athena workgroup 에 쿼리당 5GB scan cutoff, IAM 은 단일 버킷·단일 DB·단일 workgroup 으로 최소 권한입니다. Terraform 자동화는 Phase 1.5 로, 지금은 aws_initial_setup.sh bash 스크립트로 셋업합니다.
매니지먼트 job 은 적재와 시간대를 분리했습니다. Compaction 평일 18:00, Expire 일요일 19:00.
"강결합 회피" 가 핵심 컨벤션입니다. 인프라 컴포넌트 간 직접 의존을 피하고, 표준 인터페이스 — Kafka topic, S3 경로, Glue 카탈로그 — 로만 통신합니다.

---

## 29. AI 활용 방법론 — brainstorm → plan → 작은 커밋

> Claude Code superpowers 로 모든 기능을 brainstorm → spec → plan → 작은 커밋 으로 진행.

- 모든 새 기능 : `brainstorming` 스킬로 design spec → `writing-plans` 로 plan → `executing-plans`
- 작은 커밋 : task 별 1 커밋, Conventional Commits, 변경 단위를 잘게 분리
- 결정 기록 : `docs/superpowers/specs/` 의 설계 문서 = 의사결정 근거 (ADR 정식화는 Phase 1.5)
- 효과 : 6 개월 후 결정 근거를 git log + design spec 으로 재현

**시각자료**: 워크플로 그림 — idea → brainstorm → spec → plan → small commits → review → done.

**Speaker Note:**
AI 활용 방법론은 한 가지 원칙입니다. 절대 코드부터 짜지 않습니다.
모든 새 기능은 먼저 brainstorming 스킬로 design spec 을 만들고, 그 다음 writing-plans 로 실행 plan 을 만들고, 마지막에 executing-plans 로 task 별 작은 커밋을 만듭니다.
이 발표 자료 자체도 같은 흐름으로 만들어졌습니다. design spec 한 장, plan 한 장, 그 다음 슬라이드 1 장씩 작성.
작은 커밋이 중요합니다. 변경 단위를 잘게 끊고 Conventional Commits 컨벤션을 따릅니다. git log 가 그대로 결정 기록이 됩니다.
의사결정 기록은 docs/superpowers/specs 의 설계 문서가 담당합니다. DDL 을 왜 Athena 로 택했는지 같은 핵심 결정이 spec 과 핸드오프 문서에 남아 있습니다. ADR 로 정식화하는 건 Phase 1.5 항목입니다.
6 개월 후 합류한 팀원이 git log 와 docs/superpowers/specs 만 봐도 의사결정 흐름을 재현할 수 있습니다.

---

## 30. 아쉬운 점 / 부족한 점 / 궁금한 점

> 솔직한 한계 — 데이터 퀄리티 자동화 / 종목 수 3 종 / 단일 환경 → cross-region.

- 데이터 퀄리티 자동화 부족 : Great Expectations / Soda 미도입
- 종목 수 3 종 : 데모용 — Phase 1.5 에 KOSPI200 확장 필요
- Single region : DR (Disaster Recovery) 시나리오 미설계
- 궁금한 점 : 동시 종목 수 1000 → 5000 으로 가면 Kafka partition 수를 어떻게 재조정?

**시각자료**: 4 박스 — 각각 짧은 카드.

**Speaker Note:**
부족한 점을 솔직하게 공유합니다.
첫째, 데이터 퀄리티 자동화. 헬스 쿼리 4 종과 비즈니스 카운트 로그로 잡고는 있지만, Great Expectations 나 Soda 같은 정식 프레임워크는 미도입입니다.
둘째, 종목 수. Phase 1 은 데모 목적으로 3 종목만 구독합니다. KOSPI200 확장은 Phase 1.5 입니다.
셋째, Single region. ap-northeast-2 한 곳만 씁니다. 한국 서비스라 합리적이지만 DR 시나리오는 설계되어 있지 않습니다.
궁금한 점도 공유합니다. 동시 구독 종목이 1000 에서 5000 으로 늘어나면 Kafka partition 수를 어떻게 재조정해야 할지, 그리고 partition 수 변경 시 기존 메시지 순서가 어떻게 영향받는지가 가장 큰 미해결 질문입니다.

---

## 31. Phase 2 로드맵

> dbt / 자동매매 (가설) / EMR Serverless / MSK — 트리거 조건 명시.

- dbt Semantic Layer : 비즈니스 정의 (VWAP, 거래대금) 변경 빈도가 월 1+ 회 도달 시
- 자동매매 (가설) : Gold VWAP 기반 신호 → 백테스트 수익률 <TODO> → 라이브 검증
- EMR Serverless : 일 1 천만 tick 또는 Spark Standalone 단일 노드 OOM 시
- MSK : Kafka 처리량 100MB/s 도달 또는 RF=3 필요 시

**시각자료**: 4 단계 로드맵 — 각 단계의 트리거 조건과 예상 비용.

**Speaker Note:**
Phase 2 로드맵은 "언제 무엇을 도입할지 트리거 조건을 명시한 점" 이 핵심입니다.
dbt 는 비즈니스 정의 변경 빈도가 월 1 회를 넘으면 도입합니다. 지금은 Phase 1 분기라 SQL 직접 관리로 충분합니다.
자동매매는 가설 단계입니다. Gold VWAP 기반 신호로 백테스트를 돌리고 수익률을 보고 결정합니다. Phase 1 본 발표에서는 수익성 숫자를 약속하지 않습니다.
EMR Serverless 는 Spark Standalone 단일 노드가 OOM 으로 죽는 빈도가 주 1 회를 넘으면 검토합니다.
MSK 는 Kafka 처리량 100MB/s 또는 RF=3 이 필요한 시점에 검토합니다. Phase 1 은 RF=1 로 충분하지만, 상용 운영 가정이면 RF=3 으로 갈 수밖에 없습니다.
모든 도입 결정에 트리거 조건이 명시되어 있다는 게 의도된 설계입니다.

---

## 32. 회고 — 결정의 비용

> 모든 결정에 trade-off — DDL 전략 변경 / COW 수용 / 자동매매 OOS / Phase 1 속도의 비용.

- DDL 전략 : Spark CREATE → Athena SQL 전환 (개발 중반) — 일부 Iceberg key 거부 수용
- Iceberg COW : MOR 가 더 효율이지만 Athena DDL 한계로 COW 수용 → write amp 모니터링
- 자동매매 OOS : 평가자 인상 약화 vs Phase 1 안정성 — 안정성 우선
- 데이터 퀄리티 : 헬스 쿼리 vs DQ 프레임워크 — 시간 제약으로 헬스 쿼리
- **Phase 1 속도의 비용** : 컨벤션 (session.timeZone 등) 을 처음부터 굳히지 않고 빠르게 만든 결과, 발표 준비 중 6건 운영 버그 발견 → 전부 진단·수정·문서화·backfill. 비용 = D-2 의 하루. 회수 = 트러블슈팅 문서 + 재발방지 컨벤션

**시각자료**: 5 행 표 — 결정 / 비용 / 회수 시점.

**Speaker Note:**
모든 결정에는 비용이 있습니다. 솔직히 공유합니다.
첫째, DDL 전략을 개발 중반에 바꾸었습니다. 처음엔 Spark 에서 CREATE TABLE 을 호출했는데, cold start 와 단일 진실원 문제로 Athena SQL 로 전환했습니다. 그 대가로 Iceberg 네이티브 키 일부 (format-version, write.merge.mode) 가 거부되는 걸 수용했습니다.
둘째, Iceberg COW 를 그대로 받았습니다. MOR 가 MERGE 효율이 더 좋지만 Athena DDL 한계로 COW 가 기본값이 됐고, write amplification 을 모니터링하면서 임계 시 Spark bootstrap 으로 전환하기로 했습니다.
셋째, 자동매매를 빼서 평가자 인상이 약해질 위험이 있습니다. 그래도 Phase 1 안정성과 운영 가시성에 집중한 결정입니다.
넷째, 데이터 퀄리티 프레임워크 미도입. 시간 제약 때문이고, Phase 1.5 의 첫 번째 항목입니다.
다섯째, 가장 솔직한 비용입니다. Phase 1 을 빠르게 만들면서 컨벤션을 처음부터 굳히지 않았습니다. 대표적으로 Spark job 마다 session.timeZone 설정이 제각각이었는데, 발표 준비 중에 그게 Gold 가 0 rows 가 되는 timezone 버그로 터졌습니다. 이거 하나만이 아니라 — Bronze small-files 성능, DAG 자원 경합, Kafka 재시작 루프, aiokafka stuck, JMX 충돌까지 6건을 발표 D-2 에 발견했습니다. 비용은 분명합니다, 하루를 썼습니다. 하지만 그 하루에 6건 전부를 진단하고, 수정하고, backfill 하고, 트러블슈팅 문서로 남겼습니다. 평가 관점에서 보면 — 버그가 없는 게 아니라, 버그를 발견하고 체계적으로 처리하는 능력이 운영 성숙도입니다. 그게 이 항목을 회고에 정직하게 넣은 이유입니다.
이 모든 비용을 알고 결정했고, 회수 시점도 정해두었습니다.

---

## 33. Q&A

> 예상 질문 8 종 — 한 줄 답변 사전 (핸드오프 §11).

- "왜 Kafka 인가? Kinesis 아니고?" → Phase 1 로컬 컴퓨트 정책, Phase 2 트래픽 시 MSK
- "왜 Spark 인가? Glue Job 아니고?" → 비용·디버깅 단순성, 100x 시 EMR Serverless 비교
- "왜 dbt 안 썼나? / Iceberg v2 인 이유?" → dbt = Phase 2 / v2 는 행 단위 delete, Athena 도 v2 만 지원
- "장 마감 후엔 무엇이 도나?" → Compaction 평일 18:00 / Expire 일요일 19:00 (dim_symbol·DART 는 장 시작 전 배치)

**시각자료**: 5 행 표 + 추가 3 종은 핸드오프 §11 참조.

**Speaker Note:**
Q&A 슬라이드는 예상 질문에 대한 즉답 카드입니다.
다섯 가지를 슬라이드에 담았고, 추가 세 가지 (왜 Airflow 인가, 왜 Bronze 90일 후 Glacier IR 인가, 왜 ap-northeast-2 인가) 는 핸드오프 §11 에 정리되어 있습니다.
모든 답변의 공통 원칙은 두 가지입니다.
첫째, "현재 결정의 트리거 조건" 을 함께 답합니다. 그냥 "지금은 그래요" 가 아니고 "지금은 이래서 이걸 골랐고, 이런 신호가 보이면 바꿉니다" 라고 답합니다.
둘째, Phase 1 과 Phase 2 의 경계를 명확히 그립니다. "그건 Phase 2 입니다" 라고 답해도 정당화되는 결정들입니다.
감사합니다. 질문 받겠습니다.

---
