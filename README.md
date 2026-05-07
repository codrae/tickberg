# tickberg

> **Real-time Korean Stock Market Tick Data Lakehouse**
> Apache Iceberg · Kafka Streaming · Medallion Architecture · AWS S3/Glue/Athena/QuickSight

한국투자증권 Open API 실시간 체결가 데이터를 Bronze/Silver/Gold 메달리온 구조로
Iceberg Lakehouse에 적재하고, DART 공시·신용정보원 데이터와 연계해
포트폴리오 분석과 운영 가시성을 제공하는 데이터 플랫폼.

🚧 **Work in progress** — 메타코드 DE 부트캠프 8회차 최종 프로젝트

## Stack

- **Storage**: S3 + Apache Iceberg (Silver/Gold), Parquet (Bronze)
- **Catalog**: AWS Glue Data Catalog
- **Compute**: Spark (Structured Streaming + Batch)
- **Query**: Athena v3 (AWS) / Trino (Local Docker)
- **Streaming**: Kafka
- **Orchestration**: Airflow
- **BI**: QuickSight
- **Monitoring**: Prometheus + Grafana

## Data Sources

- 한국투자증권 Open API (실시간 체결가, 종목 마스터)
- DART 공시 API (재무제표, 주요공시)
- 신용정보원 데이터 (월별 신용위험 지표, 증강)

자세한 아키텍처와 설계 결정은 `docs/` 참고.

## Quick Start (Phase 1 dev)

1. `cp .env.example .env` 후 KIS / DART / AWS profile 값 채움
2. AWS 초기 셋업: `bash infra/scripts/aws_initial_setup.sh`
3. Glue/Athena DDL 실행: `bash infra/scripts/run_ddl.sh`
4. Docker 스택 기동: `docker compose -f infra/docker/docker-compose.yml up -d`
5. Airflow UI: http://localhost:8080  (admin/admin)
6. Grafana: http://localhost:3000  (admin/admin)
7. Spark UI: http://localhost:8080  (Spark master 포트 충돌 시 compose에서 변경)

자세한 architecture 는 `docs/superpowers/specs/2026-05-07-tickberg-phase1-mvp-design.md` 참고.
EOF