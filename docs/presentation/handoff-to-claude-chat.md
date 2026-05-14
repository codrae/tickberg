# tickberg 발표 준비 — Claude Chat 핸드오프

> **이 문서의 용도**
> Claude Code에서 진행한 tickberg 프로젝트 컨텍스트를 Claude Chat / NotebookLM 으로 옮겨 발표 자료(MD → PPT)를 작성하기 위한 단일 진입 문서.
> 이 MD 한 장과 [업로드 파일 인벤토리](#5-업로드-파일-인벤토리) 의 코드/DDL/문서를 함께 첨부하면 다른 Claude 세션이 프로젝트를 거의 동일하게 이해할 수 있다.
> Claude.ai 채팅, NotebookLM, Claude Code 의 메모리 시스템은 서로 분리되어 있어, 이 문서를 통한 명시적 핸드오프가 필요.

---

## 1. 프로젝트 5줄 요약

- **이름**: tickberg — Real-time Korean Stock Market Tick Data Lakehouse
- **소속**: 메타코드 DE 부트캠프 8회차 최종 프로젝트 (Public 포트폴리오)
- **미션**: 한국투자증권 실시간 체결가 + DART 공시 + 신용정보원 데이터를 Bronze/Silver/Gold 메달리온 구조로 S3 Iceberg Lakehouse 에 적재 → QuickSight 대시보드 + Prometheus/Grafana 운영 가시성
- **환경**: 컴퓨트(Spark/Kafka/Airflow) 만 로컬 Docker, 데이터·카탈로그·쿼리·BI 는 모두 AWS (단일 환경 원칙 — 로컬 MinIO/Hive 미사용)
- **일정**: 오늘 = 2026-05-08 / 1차 데모 5-10 / PPT 정합 5-14 / **본 발표 5-16**

## 2. 아키텍처 핵심 결정 5가지 (발표의 척추)

| # | 결정 | 근거 |
|---|---|---|
| 1 | **Bronze = Parquet (NOT Iceberg)** | Streaming append-only 에서 Iceberg snapshot/manifest 갱신은 순수 오버헤드. 중복 제거·UPSERT 는 Silver MERGE 에서 한 번에 처리 |
| 2 | **Silver/Gold = Iceberg** | ① 종목 마스터 MERGE INTO (액면분할·상폐 반영) ② Gold OVERWRITE 원자성 (대시보드 일관성) ③ Time-travel (Audit Trail) |
| 3 | **AWS 단일 환경** | "로컬에선 됐는데 AWS 에선 안 돼" 디버깅 차단 + 발표 시연 라이브 가능. Glue Catalog 일원화 |
| 4 | **DDL 실행 = Athena 스크립트 (NOT Spark CREATE)** | Cold start 빠름 / Spark 클러스터 의존성 0 / `.sql` 파일이 단일 진실원 / LOCATION 명시로 S3 경로 git 추적 |
| 5 | **dbt·자동매매 = Phase 2** | Phase 1 은 Lakehouse + 대시보드 + 운영 가시성에 집중 |

## 3. 데이터 흐름

```
한투 KIS Open API (WebSocket H0STCNT0)
          │
          ▼
┌─────────────────────────────┐
│ kis-producer (Python)       │  infra/docker/kis-producer/
│  - OAuth 토큰 24h 캐시      │  - 03:30 KST 자동 리프레시
│  - WebSocket subscriber     │  - heartbeat / backoff
│  - H0STCNT0 46-field parser │
│  - Kafka publisher          │
└─────────────┬───────────────┘
              │ JSON, partition key=symbol
              ▼
       Kafka topic  kis.tick.raw
              │
              ▼
┌─────────────────────────────┐
│ Spark Structured Streaming  │  code/pipelines/bronze/
│  trigger 1min, append       │  bronze_kis_tick_streaming.py
└─────────────┬───────────────┘
              ▼
     S3 Bronze Parquet  (dt=YYYY-MM-DD/hr=H/)
     tickberg.bronze_kis_tick_raw  (Glue Partition Projection)
              │
              ▼
┌─────────────────────────────┐
│ Spark batch MERGE (dedup)   │  code/pipelines/silver/
│ Airflow DAG, 분당 트리거    │  bronze_to_silver_kis_tick.py
└─────────────┬───────────────┘
              ▼
     S3 Silver Iceberg
     tickberg.silver_kis_tick_clean   (hour partition)
     tickberg.silver_dim_symbol       (market partition, daily MERGE)
              │
              ▼
┌─────────────────────────────┐
│ Spark batch OVERWRITE       │  code/pipelines/gold/
│ 1m VWAP/OHLC 집계           │  silver_to_gold_vwap.py
└─────────────┬───────────────┘
              ▼
     S3 Gold Iceberg
     tickberg.gold_symbol_vwap_1m     (hour partition)
              │
              ▼
   Athena (workgroup tickberg-wg, 5GB scan limit)
              │
              ▼
   QuickSight  ─  비즈니스 KPI 탭 + 운영 메트릭 탭

   [모니터링 평행선]
   Spark / Kafka / Producer  →  Prometheus  →  Grafana
```

## 4. 평가 기준 (8회차 가이드)

발표는 이 4축으로 평가됨. PPT/Q&A 모두 이 축에 맞춰 답할 것.

1. **운영 가시성** — 운영자가 5분 안에 헬스체크 가능한가?
2. **100x 스케일 사고력** — 일 100만 → 1억 이벤트 시 어디가 깨지고 어떻게 대응 (설계만)
3. **Iceberg 필요성** — 왜 그냥 Parquet + Glue 가 아닌가
4. **협업·지속가능성** — 6개월 후 새 팀원이 합류 가능한가

### Phase 1 MVP 필수 요건
1. Iceberg 활용 (Silver/Gold)
2. 메달리온 Bronze/Silver/Gold 3계층 분리
3. Iceberg 매니지먼트 자동화 — Compaction / Expire Snapshots / Orphan Cleanup 중 최소 1개를 Airflow DAG 로
4. QuickSight 대시보드 — 비즈니스 KPI 탭 + 운영 메트릭 탭

## 5. 업로드 파일 인벤토리

다음 파일들을 Claude Chat 또는 NotebookLM 에 함께 첨부할 것. **굵은 글씨 = 우선순위 높음**.

### 5-1. 코드 — 파이프라인 (가장 중요)
- **`code/pipelines/bronze/bronze_kis_tick_streaming.py`** — Kafka → S3 Bronze Parquet, 1min trigger
- **`code/pipelines/silver/bronze_to_silver_kis_tick.py`** — dedup MERGE
- `code/pipelines/silver/dim_symbol_daily.py` — 종목 마스터 일일 MERGE
- **`code/pipelines/gold/silver_to_gold_vwap.py`** — 1분 VWAP/OHLC OVERWRITE

### 5-2. 코드 — 프로듀서
- `infra/docker/kis-producer/main.py` — 엔트리포인트
- `infra/docker/kis-producer/src/kis_auth.py` — OAuth + 토큰 캐시
- `infra/docker/kis-producer/src/kis_websocket.py` — H0STCNT0 구독 + heartbeat/backoff
- `infra/docker/kis-producer/src/parser.py` — 46-field canonical parser
- `infra/docker/kis-producer/src/kafka_publisher.py` — symbol partition key
- `infra/docker/kis-producer/src/token_refresher.py` — 03:30 KST 자동 리프레시
- `infra/docker/kis-producer/src/health_metrics.py` — Prometheus 메트릭

### 5-3. DDL (4개)
- **`code/ddl/bronze/kis_tick_raw.sql`** — External table + Glue Partition Projection
- **`code/ddl/silver/kis_tick_clean.sql`** — Iceberg, hour partition
- `code/ddl/silver/dim_symbol.sql` — Iceberg, market partition
- **`code/ddl/gold/symbol_vwap_1m.sql`** — Iceberg, hour partition

### 5-4. 헬스체크 쿼리
- `code/health-queries/01_bronze_freshness.sql`
- `code/health-queries/02_silver_dedup_rate.sql`
- `code/health-queries/03_symbol_coverage.sql`
- `code/health-queries/04_gold_partition_completeness.sql`

### 5-5. 오케스트레이션 (Airflow DAG)
- `orchestration/dags/_common.py`
- `orchestration/dags/bronze_to_silver_kis.py`
- `orchestration/dags/dim_symbol_daily.py`
- `orchestration/dags/silver_to_gold_vwap.py`
- **`orchestration/dags/iceberg_compaction.py`** — MVP 요건 #3 충족 DAG

### 5-6. 인프라
- `infra/scripts/run_ddl.sh` — Athena 로 DDL 일괄 적용
- `infra/scripts/aws_initial_setup.sh` — S3 버킷·Glue DB·Athena workgroup 생성
- `infra/scripts/kafka_create_topics.sh`
- `infra/docker/docker-compose.yml`
- `infra/docker/spark/Dockerfile` — Iceberg 1.5.2 + AWS bundle + Kafka + Prometheus servlet
- `infra/docker/airflow/Dockerfile`

### 5-7. 문서 (필수 첨부)
- **`CLAUDE.md`** — 프로젝트 정체성·미션·결정·가드레일
- **`docs/superpowers/specs/2026-05-07-tickberg-phase1-mvp-design.md`** — Phase 1 설계
- `docs/superpowers/plans/2026-05-07-tickberg-phase1-mvp.md` — Phase 1 plan
- 본 문서 (`docs/presentation/handoff-to-claude-chat.md`)

## 6. NotebookLM 활용 — Q&A 시뮬레이션 질문 모음

이 질문들을 NotebookLM 에 한 묶음씩 던지고 답변을 정리. 답변 약점 = 발표 보완 포인트.

### 6-1. 운영 가시성 (5분 헬스체크)
1. Bronze 적재가 멈췄는지를 평가자가 가장 빨리 확인할 수 있는 경로는? (Athena 쿼리 한 줄? Grafana 패널? Airflow UI?)
2. Silver MERGE 가 실패했을 때 알람·로그 경로는? 누가 무엇을 보는가?
3. Kafka consumer lag 가 비정상적으로 늘어나는 것을 어떻게 감지하나?
4. S3 비용이 갑자기 튀면 어디서 알게 되나? (Budgets 외에)
5. 어제 Gold 분봉이 일부 빠졌다는 사실을 BI 사용자보다 먼저 알 수 있나?
6. 운영 메트릭 탭 슬라이드에 들어가야 할 패널 5개는?

### 6-2. 100x 스케일 사고력
7. 일 100만 → 1억 이벤트 시 Bronze 1분 trigger 는 어떻게 깨지나? 첫 번째로 깨지는 자원은?
8. Silver MERGE 비용은 어떻게 폭증하나? (write amplification — copy-on-write 시 partition rewrite 비용)
9. Athena 5GB scan cutoff 가 막히는 시점·쿼리 패턴은?
10. Glue 무료 티어 (월 100만 요청) 를 넘기는 시점?
11. Iceberg snapshot 누적으로 메타데이터 비용이 데이터 비용을 추월하는 임계는? (대응 = Compaction + Expire Snapshots DAG)
12. 100x 시 EMR Serverless / MSK / Flink 전환 트리거 조건은? (Phase 2 로 어떤 신호가 보이면 넘기는가)

### 6-3. Iceberg 필요성
13. Bronze 는 왜 Parquet 인가? Iceberg 안 쓴 진짜 이유?
14. Silver 가 Iceberg 여서 얻는 3가지 가치를 코드/DDL 의 어디서 확인할 수 있나?
15. Gold 가 Iceberg 여서 얻는 가치는? (OVERWRITE 원자성이 BI 일관성과 어떻게 연결되나)
16. "그냥 Parquet + Glue projection" 으로 Silver/Gold 도 가능하지 않나? 어디서 무너지나?
17. Iceberg 매니지먼트 (Compaction / Expire / Orphan) 중 가장 먼저 자동화한 이유는?

### 6-4. 협업·지속가능성
18. 6개월 후 합류한 팀원이 Bronze→Silver 흐름을 디버깅한다면 어떤 순서로 무엇을 보나?
19. DDL 변경은 어떻게 추적되나? (`code/ddl/*.sql` git diff = 스키마 변경)
20. 비즈니스 정의 (VWAP, 거래대금) 변경 요청이 오면 어디 한 곳을 고치는가?
21. 시크릿·자격증명은 어디에 있고, Phase 2 운영 전환 시 어떻게 옮길 계획인가?
22. CLAUDE.md 의 코딩 컨벤션·가드레일은 누가 어떻게 강제하나?

### 6-5. 발표 흐름 검증 (PPT 초안 업로드 후)
23. 이 슬라이드 흐름에서 평가자가 가장 의심할 지점은?
24. 30 초 안에 핵심을 전달해야 한다면 슬라이드 어떤 순서?
25. 기술 외 청중도 이해하려면 어느 부분이 추상화되어야 하는가?
26. "왜 그냥 RDS 에 Append 하면 안 되나" 질문이 왔을 때 한 슬라이드로 답할 자료는?

## 7. PPT 골격 제안 (10–12 슬라이드)

각 슬라이드는 5줄 이하 + 시각자료 1개 원칙.

| # | 슬라이드 | 핵심 메시지 |
|---|---|---|
| 1 | 표지 | tickberg — Real-time Korean Stock Tick Lakehouse |
| 2 | 문제 정의 | 한국 주식 실시간 체결을 BI·운영 가시성과 함께 안전히 적재 |
| 3 | 아키텍처 한 장 | (3절 다이어그램 슬라이드화) |
| 4 | 핵심 결정 5가지 | 위 [2절 표](#2-아키텍처-핵심-결정-5가지-발표의-척추) |
| 5 | 데이터 흐름 라이브 데모 | KIS WebSocket → Kafka → Bronze → Silver → Gold → Athena 한 큐 |
| 6 | Iceberg 매니지먼트 자동화 | Compaction DAG 코드 한 컷 + before/after 메타 비용 |
| 7 | 운영 가시성 | Grafana 패널 + Athena 헬스체크 쿼리 4종 |
| 8 | 100x 스케일 시 깨질 지점 | 3개 지점 + 대응 (1분 trigger / Silver write amp / Snapshot 누적) |
| 9 | 협업·지속가능성 | git diff = 스키마 변경, CLAUDE.md, ADR |
| 10 | Phase 2 로드맵 | dbt / 자동매매 / EMR Serverless 트리거 조건 |
| 11 | 회고 — 결정의 비용 | DDL 실행 전략 변경 (Spark→Athena), copy-on-write 수용 등 솔직한 trade-off |
| 12 | Q&A | (6절 질문에 대비) |

## 8. 작업 절차

```
[Claude Chat]                       [NotebookLM]                  [PPT 도구]
  │                                   │                             │
  ① 본 MD + 5절 파일 첨부             │                             │
  ② 6절 Q&A 묶음으로 답변 정리       │                             │
  ③ 7절 PPT 골격에 답변 채우기       │                             │
  ④ 슬라이드별 본문 MD 출력           ───→ ⑤ 본 MD + 코드 + 슬라이드 MD 업로드 │
                                            ⑥ Audio Overview 듣기  │
                                            ⑦ 6-5절 흐름 검증     │
                                                                   │
                                            ⑧ 약점 보강 후 슬라이드 MD ──→ Gamma 입력 → 자동 슬라이드
                                                                                        │
                                                                          ⑨ Google Slides 옮겨 디자인 정렬
                                                                                        │
                                                                          ⑩ 본 발표 5-16
```

## 9. 현재 작업 상태 (인계 시점 기준)

- ✅ 프로듀서·Bronze streaming·Silver MERGE·Gold OVERWRITE 코드 모두 작성
- ✅ Bronze DDL Athena 적용 완료
- ✅ Silver `dim_symbol` / Silver `kis_tick_clean` Athena 적용 완료
- ⏳ Gold `gold_symbol_vwap_1m` Athena 적용 — 마지막 단계 진행 중
- ⏳ ADR 0001: *DDL 실행 전략 — Athena vs Spark* — 본 세션에서 작성 예정
- ⏳ Iceberg Compaction DAG 검증 (MVP 요건 #3)
- ⏳ QuickSight 대시보드 (MVP 요건 #4)

## 10. 발표용 핵심 사실 — 절대 헷갈리지 말 것

- Bronze 는 **Parquet** (Iceberg 아님). 이유는 streaming snapshot 오버헤드.
- Silver/Gold 는 **Iceberg**. 이유는 MERGE / OVERWRITE 원자성 / Time-travel.
- Bronze 외부 테이블은 **Glue Partition Projection** 사용 — `MSCK REPAIR` 불필요.
- DDL 은 **Athena 에서 실행** — Spark 가 아님. 이유는 cold start / 의존성 / 단일 진실원.
- Athena DDL 은 Iceberg 네이티브 키 일부 (`format-version`, `write.merge.mode` 등) 를 거부 → Phase 1 은 copy-on-write 기본값 수용. write amplification 이 측정상 문제될 때 Spark bootstrap 으로 사후 변경.
- 모든 시크릿은 `.env` 만, AWS 자격증명은 `~/.aws/credentials` 의 `default` 프로파일 사용 (CLAUDE.md 는 `tickberg` 프로파일을 가정하나 실제 셋업은 `default`).
- 비용 가드레일: 월 $20 Budgets 알람, Athena workgroup 5GB scan cutoff.

## 11. 자주 받을 질문 — 한 줄 답변 사전 (Q&A 카드)

| 질문 | 한 줄 답변 |
|---|---|
| 왜 Kafka 인가? Kinesis 아니고? | Phase 1 은 로컬 컴퓨트 정책. Phase 2 트래픽 증가 시 MSK 전환 트리거 명시 |
| 왜 Airflow 인가? Step Functions 아니고? | DAG 시각화 + 로컬 디버깅 + Phase 2 분리 시점에 EventBridge 검토 |
| 왜 Spark 인가? Glue Job / EMR Serverless 아니고? | Phase 1 은 비용·디버깅 단순성. 100x 시 EMR Serverless 비교표 (Phase 2) |
| 왜 dbt 안 썼나? | Phase 1 은 PySpark + SQL DDL 직접. dbt Semantic Layer 는 Phase 2 |
| 왜 Bronze 90 일 후 Glacier IR 인가? | KIS API 재호출 비용 vs Glacier IR 보관 비용 비교 (간단 계산 슬라이드) |
| 왜 ap-northeast-2 인가? | KIS API·발표 대역 모두 한국. 데이터 국적 정합성 |
| Iceberg v2 인 이유? | 행 단위 delete 지원 → MERGE 효율. Athena 도 v2 만 지원 |
| 장 마감 후엔 무엇이 도나? | Compaction / Expire Snapshots / dim_symbol daily MERGE / DART 일배치 |

## 부록 — Claude Chat 첫 프롬프트 템플릿

```
이 MD 와 첨부된 코드/DDL 파일들을 기반으로 tickberg 프로젝트의 발표 자료를 만들고 싶어.
먼저 6절의 Q&A 시뮬레이션 중 [원하는 카테고리] 의 질문들에 대해
첨부 파일들을 근거로 답변해줘. 추측 금지, 파일 인용 필수.
이후 답변을 7절 PPT 골격의 해당 슬라이드 본문으로 정리해줘.
```
