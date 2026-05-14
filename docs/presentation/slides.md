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
- ③ AWS 단일 환경 — Glue Catalog 일원화, 로컬 MinIO/Hive 미사용
- ④ DDL = Athena SQL — Spark cold start 회피, `.sql` 단일 진실원
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
