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
