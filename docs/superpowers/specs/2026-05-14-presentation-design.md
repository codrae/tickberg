# tickberg 최종 발표 자료 — Design Spec

> **상태**: Brainstorming 완료, 콘텐츠 작성 대기
> **작성일**: 2026-05-14
> **발표일**: 2026-05-16
> **PPT 정합 마감**: 2026-05-14 24:00

---

## 1. 목적

메타코드 DE 부트캠프 8회차 최종 발표용 슬라이드 콘텐츠를 단일 Markdown 파일로 작성한다.
이 문서는 슬라이드 구조·각 슬라이드의 콘텐츠 사양·작성 규칙을 정의하고, 다음 단계인
`writing-plans` 가 슬라이드 1장씩 채우는 실행 계획을 만들 수 있도록 한다.

## 2. 결정 사항 (사용자 합의)

| # | 결정 | 사유 |
|---|---|---|
| 1 | **분량 = 25분 / 약 33장 (스탠다드)** | 평가 4축을 모두 다루되 데모 안정성 확보. 핸드오프 10–12장은 너무 압축, 40+장은 시간 초과. |
| 2 | **출력 형식 = 단일 MD (`docs/presentation/slides.md`)** | Gamma 한 번에 붙여넣기. `---` 으로 슬라이드 구분, H1 제목, bullet ≤ 4줄, `Speaker Note:` 블록 포함. |
| 3 | **수치 = placeholder 우선** | 실측 가능 항목 (Bronze 건수, snapshot 수 등) 은 차후 Athena 조회로 교체. `<TODO: 실측값>` 표기. |
| 4 | **자동매매 = Phase 2 로드맵 1장** | CLAUDE.md 의 OOS 정책 준수. 본문에는 수익성 가설을 두지 않고, 마지막 로드맵 슬라이드에서만 가설 형태로 언급. |
| 5 | **흐름 = 평가 4축 우선 (Approach A)** | 운영 가시성 / 100x / Iceberg 필요성 / 협업 4축이 슬라이드 흐름에서 자연스럽게 드러나도록 배치. Iceberg 이유를 메달리온 앞에 둠. |

## 3. 평가 4축 매핑 (발표의 척추)

| 평가축 | 답하는 슬라이드 |
|---|---|
| 운영 가시성 (5분 헬스체크) | 13 Airflow DAG · 14 로그 · 15 검증 쿼리 · 18 운영 메트릭 대시보드 · 25 Prometheus/Grafana |
| 100x 스케일 사고력 | 26 Small Files + Scale out · 27 OOM·S3 네트워크 장애 · 31 Phase 2 (EMR Serverless 트리거) |
| Iceberg 필요성 | 06 Lake/Warehouse 한계 · 07 MERGE/Time-travel · 08 Bronze=Parquet 결정 · 19–24 운영 매니지먼트 |
| 협업·지속가능성 | 05 핵심 결정 5가지 · 28 코드/인프라 분리 · 29 AI 활용 방법론 · 32 회고 |

## 4. 슬라이드 골격 (33장)

각 슬라이드는 1줄 메시지 + bullet ≤ 4줄 + Speaker Note 6–10줄 원칙.

| # | 제목 | 핵심 메시지 | 시각자료 |
|---|---|---|---|
| 01 | 표지 | tickberg — Real-time Korean Stock Tick Lakehouse | 표지 디자인 |
| 02 | 문제·도메인·KPI | 실시간 체결을 BI·운영 가시성과 함께 안전히 적재하는 것이 왜 어려운가 | 평가 4축 표 |
| 03 | Data Source 3종 + 규모 | KIS WebSocket / DART / 신용정보원 — 일 건수·페이로드·갱신 주기 | 3열 비교표 |
| 04 | 아키텍처 한 장 | 컴퓨트는 Local Mac, 데이터·카탈로그·BI 는 AWS — 단일 환경 원칙 | 핸드오프 §3 다이어그램 |
| 05 | 핵심 결정 5가지 | Bronze=Parquet / Silver·Gold=Iceberg / AWS-only / Athena DDL / dbt·자동매매 Phase 2 | 5행 표 |
| 06 | Iceberg 필요성 ① | Data Warehouse·Data Lake 의 한계 — 갱신·시점성·스키마 진화 | 비교 매트릭스 |
| 07 | Iceberg 필요성 ② | MERGE INTO (액면분할 반영) + Time-travel (Audit) 의 구체 시나리오 | 코드 한 컷 |
| 08 | Bronze 스키마 | 왜 Parquet — streaming snapshot 오버헤드 차단 | DDL 한 컷 |
| 09 | Silver 스키마 | COW 수용 + MOR 전환 트리거 명시 | DDL + Athena Iceberg key 제약 |
| 10 | Gold 스키마 | Overwrite 원자성 + 1분봉 일괄집계 (증분집계 X) | DDL + 집계 SQL |
| 11 | Kafka topic·파티션 | `kis.tick.raw` 1 topic, partition key=symbol, metadata 범위 | 토픽 설계도 |
| 12 | Kafka 튜닝값 | acks=all / linger.ms=20 / compression=lz4 / max.request.size | 4행 표 + 근거 |
| 13 | Airflow DAG 설계 | 분당 트리거 / 일배치 / Compaction (장 마감 후) — 시간대 분리 | DAG 그래프 |
| 14 | 로그 설계 | 무엇을 남겼나 — task_instance · sla · 비즈니스 카운트 | 로그 샘플 |
| 15 | 검증 쿼리 4종 | Bronze freshness / dedup rate / symbol coverage / Gold partition completeness | 쿼리 컷 |
| 16 | 데이터 퀄리티 체크 | NULL / 중복 / 시점 일관성 — 헬스 쿼리로 어떻게 잡나 | 체크 매트릭스 |
| 17 | Superset Dashboard ① | 비즈니스 KPI — VWAP / 거래대금 / 종목 랭킹 | 대시보드 스샷 |
| 18 | Superset Dashboard ② | 운영 메트릭 — Bronze lag / Silver dedup / Snapshot 누적 | 운영 패널 스샷 |
| 19 | 운영 ① Snapshot/Time-travel | 시점 조회 실제 사용 케이스 | snapshot id 쿼리 |
| 20 | 운영 ② Rollback | "자주 발생하면 파이프라인 설계 의심" 원칙 | rollback 절차 |
| 21 | 운영 ③ Expire 정책 | 최소 3일 + Spark 리소스 고려 + 테이블별 분리 | 정책 표 |
| 22 | 운영 ④ Remove Orphan + Compaction | min-files 변수 분리·쿼리 플랜 영향 | before/after 스캔량 |
| 23 | 운영 ⑤ 테이블별 정책 | 일일 루틴 매트릭스 — Bronze / Silver / Gold 별 | 매트릭스 |
| 24 | 운영 ⑥ 동시성 충돌 회피 | streaming + batch 같이 도는 패턴 | 충돌 회피 다이어그램 |
| 25 | Prometheus + Grafana | 장애 인지 시간·운영 가시성 — 어떤 알람 / 무엇을 보나 | Grafana 스샷 |
| 26 | 예상 문제 ① Small Files / Scale out | 100만 → 1억 이벤트 시 어디부터 깨지나 | 깨지는 순서 다이어그램 |
| 27 | 예상 문제 ② OOM / S3 네트워크 장애 | App 서버 OOM / S3 일시 장애 시 동작 | failure mode 표 |
| 28 | 코드·인프라 노력 | 강결합 회피 / DDL 분리 / terraform / Athena 워크그룹 제한 / 장 마감 후 매니지먼트 | 7행 표 |
| 29 | AI 활용 방법론 | brainstorm → plan → 작은 커밋 — Claude Code superpowers | 워크플로 그림 |
| 30 | 아쉬운 점 / 부족한 점 | 솔직한 한계 — 데이터 퀄리티 모니터링 자동화 부족 등 | bullet |
| 31 | Phase 2 로드맵 | 자동매매 수익성 가설 / dbt / EMR Serverless / MSK 전환 트리거 | 로드맵 |
| 32 | 회고 — 결정의 비용 | DDL 실행 전략 변경 / COW 수용 등 솔직한 trade-off | 비교 표 |
| 33 | Q&A | 핸드오프 §11 한 줄 답변 사전 | — |

## 5. 작성 규칙

### 5.1 슬라이드 단위 콘텐츠 사양

각 슬라이드 MD 블록은 아래 구조를 따른다.

```markdown
## NN. <Title>

> <1줄 핵심 메시지 — 평가자가 1초에 잡는 결론>

- bullet 1 (≤ 한 줄)
- bullet 2
- bullet 3
- bullet 4

**시각자료**: <표 / 다이어그램 / 코드 컷 placeholder 설명>

**Speaker Note:**
6~10줄. 청중에게 말로 전달할 내용. 슬라이드 본문 그대로 읽지 않고, 사유와 사례를 보탬.
평가 4축 중 어느 축에 답하는지 1회 명시 권장 (예: "이 슬라이드는 운영 가시성 축에 답합니다").

---
```

### 5.2 콘텐츠 톤

- **언어**: 한국어 (CLAUDE.md 정책).
- **슬라이드 bullet 톤**: 짧은 명사·서술 종결 — "~수용", "~차단", "Phase 2 전환", "MERGE INTO 필수". 문장형 회피.
- **1줄 핵심 메시지 톤**: 평서체 한 줄 — "Bronze 는 Parquet, snapshot 오버헤드 차단."
- **Speaker Note 톤**: 합니다체 (발표자가 입에서 그대로 나오는 문장). 핸드오프 §11 의 한 줄 답변과 연결.
- **수치 = placeholder** : 모르면 `<TODO: 실측값>` 으로 표기, 추측 금지.
- **장 시간대 vs 장 마감 후 분리 명시** : 모든 스케줄·자원·모니터링 표현에서 두 구간 분리 표기 (memory: `feedback_market_hours_separation`).
- **AWS 비용 = worst-case framing** : 낙관 단일값 X, Phase 1·100x 두 framing 명시 (memory: `feedback_aws_cost_estimates`).
- **KIS 관련 필드·컬럼** : KIS MCP 로 확인 후 작성, 추정 금지 (memory: `feedback_kis_mcp_active_use`).

### 5.3 시각자료 정책

- Gamma 입력 후 다듬을 예정이므로, 슬라이드에는 **이미지 경로가 아닌 placeholder 설명문** 만 둔다.
  - 예: `**시각자료**: docs/presentation/img/architecture.png — 핸드오프 §3 다이어그램을 PPT 형 박스 다이어그램으로`
- Mermaid 는 Gamma 가 일부만 렌더링 → ASCII / 표 / placeholder 위주. 별도 이미지로 갈 항목은 placeholder 만.
- 다이어그램 출처는 핸드오프 §3 / CLAUDE.md / 실제 코드를 기반으로 한다.

### 5.4 출력 파일 구조

```
docs/presentation/
├── handoff-to-claude-chat.md   (기존)
├── slides.md                    (이번에 작성 — Gamma 입력용 단일 파일)
└── img/                         (이미지 placeholder 경로, 필요시)
```

## 6. 참조 자료

| 무엇 | 어디 |
|---|---|
| 프로젝트 정체성·결정 | `CLAUDE.md` |
| Phase 1 설계 | `docs/superpowers/specs/2026-05-07-tickberg-phase1-mvp-design.md` |
| 핸드오프 (PPT 골격·Q&A·핵심 사실) | `docs/presentation/handoff-to-claude-chat.md` |
| Bronze 파이프라인 | `code/pipelines/bronze/bronze_kis_tick_streaming.py` |
| Silver MERGE | `code/pipelines/silver/bronze_to_silver_kis_tick.py` |
| Gold OVERWRITE | `code/pipelines/gold/silver_to_gold_vwap.py` |
| DART Bronze ingest | `code/pipelines/bronze/dart_disclosure_ingest.py` |
| DART Silver merge | `code/pipelines/silver/dart_silver_merge.py` |
| Iceberg Compaction DAG | `orchestration/dags/iceberg_compaction.py` |
| DDL 4종 | `code/ddl/{bronze,silver,gold}/*.sql` |
| 헬스 쿼리 | `code/health-queries/*.sql` |
| Superset 대시보드 | `dashboard/superset/`, `dashboard/screenshots/` |

## 7. 다음 단계

1. 사용자가 본 스펙 검토·승인
2. `writing-plans` 스킬로 슬라이드 1장씩 작성하는 실행 계획 수립
3. 슬라이드 콘텐츠 작성 — 5.1 규칙에 따라 1장씩
4. (선택) PPT 정합 마감 24:00 전 일부 placeholder 를 Athena 조회로 실측 교체

## 8. Out of Scope

- 디자인 톤·컬러·폰트 (Gamma·Google Slides 단계)
- 라이브 데모 스크립트
- 발표자 리허설용 별도 cue card
- 발표 후 Q&A 응답 문서화 (핸드오프 §11 로 갈음)
