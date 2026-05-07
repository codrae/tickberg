# tickberg — Project Context

## 정체성
- 메타코드 DE 부트캠프 8회차 최종 프로젝트 (Public 포트폴리오)
- 도메인: 한국 주식 시장 실시간 체결 데이터 Lakehouse
- 컨셉명: **PortfolioStream** — Real-time Korean Stock Market Tick Data Lakehouse

## 미션
한국투자증권 실시간 체결가 + DART 공시 + 신용정보원 데이터를
Bronze/Silver/Gold 메달리온 구조로 S3 Iceberg Lakehouse에 적재하고,
QuickSight 대시보드와 Prometheus/Grafana 운영 가시성을 제공하는 데이터 플랫폼.

## 환경 구성 — AWS 단일 환경
로컬 시뮬레이션 (MinIO / Hive Metastore 등) 사용하지 않음.
컴퓨트(Spark/Kafka/Airflow)만 로컬 Docker, 데이터·카탈로그·쿼리·BI는 모두 AWS.
이유: "로컬에선 됐는데 AWS에선 안 돼" 디버깅 차단 + 발표 시연 라이브 가능.

| 컴포넌트 | 도구 | 위치 |
|---|---|---|
| 메시지 큐 | Apache Kafka (OSS, KRaft 모드) | 로컬 Docker |
| 스트리밍·배치 처리 | Apache Spark | 로컬 Docker (Standalone) |
| 오케스트레이션 | Apache Airflow | 로컬 Docker (PostgreSQL 백엔드) |
| 스토리지 | AWS S3 | AWS (ap-northeast-2) |
| 테이블 포맷 | Apache Iceberg (Silver/Gold), Parquet (Bronze) | S3 |
| 카탈로그 | AWS Glue Data Catalog | AWS |
| 쿼리 엔진 | AWS Athena v3 | AWS |
| BI | AWS QuickSight | AWS |
| 모니터링 | Prometheus + Grafana | 로컬 Docker |

## 아키텍처 핵심 결정
- **Bronze = Parquet (NOT Iceberg)** — Streaming append-only에서 snapshot/manifest 갱신은 순수 오버헤드. 중복 제거·UPSERT는 Silver MERGE에서 한 번에 처리.
- **Silver/Gold = Iceberg** — 세 가지 가치: ① 종목 마스터 MERGE INTO (액면분할·상폐), ② Gold OVERWRITE 원자성 (대시보드 일관성), ③ Time-travel (Audit Trail).
- **AWS 단일 환경** — 로컬 MinIO/Hive Metastore 미사용, Glue Catalog로 일원화.
- **dbt = Phase 2** — Phase 1은 PySpark + SQL DDL 직접 작성.
- **자동매매 = Phase 2** — Phase 1은 Lakehouse + 대시보드 + 운영 가시성에 집중.

## 데이터 소스 (Phase 1 한정 3개)
1. 한국투자증권 Open API — WebSocket 실시간 체결가 + REST 종목 마스터
2. DART 공시 API — 분기 재무제표 + 주요 공시
3. 신용정보원 데이터 — 월별 증강 데이터 (xlsx)

## Phase 1 MVP 필수 요건
1. Iceberg 테이블 활용 (Silver/Gold)
2. 메달리온 Bronze/Silver/Gold 3계층 분리
3. Iceberg 매니지먼트 자동화 — Compaction / Expire Snapshots / Orphan Cleanup 중 최소 1개를 Airflow DAG로
4. QuickSight 대시보드 — 비즈니스 KPI 탭 + 운영 메트릭 탭

## 평가 기준 (8회차 가이드)
- 운영 가시성 — 운영자가 5분 안에 헬스체크 가능?
- 100x 스케일 사고력 — 일 100만→1억 이벤트 시 어디가 깨지고 어떻게 스케일아웃 (설계만)
- Iceberg 필요성 — 왜 그냥 Parquet + Glue가 아닌가
- 협업·지속가능성 — 6개월 후 새 팀원이 합류 가능한가

## Out of Scope (Phase 2 이후)
- dbt Semantic Layer
- 자동매매 (Signal Generator + Order Executor + Risk Manager)
- Alpaca / 미국 주식 / 암호화폐
- EMR Serverless / MSK / Flink 전환
- Trino (필요 시 Phase 2에서 추가)

## AWS 비용·자원 가드레일
처음부터 AWS만 쓰므로 비용 폭주 방지가 중요.
- S3 버킷: `tickberg-lakehouse` (단일, ap-northeast-2 서울 리전)
- S3 Lifecycle: Bronze raw 90일 후 Glacier IR 이동, Silver/Gold는 Standard 유지
- Athena: workgroup 단위로 쿼리당 최대 스캔 5GB 한도 설정
- Glue: 무료 티어 (월 100만 요청) 안에서 운영
- 비용 알람: AWS Budgets로 월 $20 초과 시 이메일 알림
- 미사용 파티션·테스트 데이터는 즉시 삭제

## 보안·시크릿
- 모든 시크릿은 `.env`에만, git 커밋 금지 (`.env.example`만 커밋)
- AWS 자격증명: 로컬 `~/.aws/credentials` 프로파일 사용 (`AWS_PROFILE=tickberg`)
- 한국투자증권 API 키, DART API 키는 `.env`에만
- IAM 사용자는 최소 권한 원칙 (S3 단일 버킷·Glue 단일 DB·Athena workgroup 한정)
- Phase 2 운영 전환 시 AWS Secrets Manager 이전 검토

## 코딩 컨벤션
- Python 3.11+
- 포매팅 `black`, lint `ruff`, 타입힌트 필수
- SQL은 `code/ddl/` 또는 `code/health-queries/`에 `.sql` 파일로 관리
- Spark job은 `code/pipelines/<layer>/`에 모듈별 분리
- 커밋 메시지: Conventional Commits (feat/fix/chore/docs/refactor/test)

## Working Style with Claude Code
- 새 기능은 곧장 코드부터 짜지 말 것 — Superpowers `brainstorm` → `write-plan` → `execute-plan` 순서 준수
- 데이터 파일(.parquet, .csv, .xlsx)은 절대 git 커밋 금지
- AWS 비용 발생 가능성 있는 작업(Athena 쿼리, S3 대용량 적재 등)은 사용자에게 먼저 확인
- 한국어로 응답
