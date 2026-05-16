# tickberg Phase 1 MVP Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 한국투자증권 실시간 체결가를 Kafka → Spark Streaming → S3 Iceberg Lakehouse → Athena → QuickSight 한 줄기로 통과시켜 5/10 1차 발표용 working E2E (Milestone 1A) 와 5/14 자정 PPT 마감 + 5/16 최종 발표용 풍부화 (Milestone 1B) 두 단계를 cumulative 하게 구축한다.

**Architecture:** AWS 단일 환경 (S3 + Glue + Athena + QuickSight, ap-northeast-2) + 컴퓨트만 로컬 Docker (Kafka KRaft, Spark Standalone, Airflow LocalExecutor, Prometheus, Grafana). Bronze=Parquet, Silver/Gold=Iceberg v2. Streaming Bronze 적재 + Airflow 5분 cycle Bronze→Silver MERGE INTO + Silver→Gold hour partition OVERWRITE.

**Tech Stack:** Python 3.11+, PySpark 3.5+, Apache Iceberg 1.4+, Apache Kafka (KRaft), Apache Airflow 2.9+, AWS S3 / Glue / Athena v3 / QuickSight, Prometheus + Grafana, Docker Compose, pytest.

**Spec:** `docs/superpowers/specs/2026-05-07-tickberg-phase1-mvp-design.md`

---

## Milestone 1A — 5/10 일 1차 발표용 Working E2E (5/7 목 저녁 ~ 5/9 토 21:00 cutoff)

목표: 한투 실시간 체결가가 Bronze (Parquet) → Silver (Iceberg MERGE dedup) → Gold (Iceberg hour OVERWRITE) → Athena/QuickSight 까지 한 줄기로 흐르고 4-tier 운영 가시성 minimum 이 동작.

---

### Task 1: 프로젝트 skeleton + .env.example + .gitignore

**Files:**
- Create: `.gitignore`
- Create: `.env.example`
- Modify: `README.md` (간단 셋업 가이드 추가)

- [ ] **Step 1: `.gitignore` 작성**

```gitignore
# Python
__pycache__/
*.py[cod]
*.egg-info/
.venv/
venv/
.pytest_cache/
.mypy_cache/
.ruff_cache/

# IDE
.idea/
.vscode/
*.swp

# Secrets
.env
.env.local
*.pem
*.key

# Data files (CLAUDE.md: 데이터 파일 git 커밋 금지)
*.parquet
*.csv
*.xlsx
*.json.gz

# Spark / Airflow
spark-warehouse/
metastore_db/
derby.log
logs/
airflow.cfg
airflow.db
webserver_config.py

# Local Docker volumes
volumes/
.terraform/
*.tfstate
*.tfstate.backup

# Build artifacts
dist/
build/
.coverage
htmlcov/
```

- [ ] **Step 2: `.env.example` 작성**

```dotenv
# AWS (로컬 ~/.aws/credentials 의 'tickberg' profile 을 사용)
AWS_PROFILE=tickberg
AWS_REGION=ap-northeast-2
S3_BUCKET=tickberg-lakehouse
GLUE_DATABASE=tickberg
ATHENA_WORKGROUP=tickberg-wg

# KIS Open API (https://apiportal.koreainvestment.com)
KIS_APP_KEY=__FILL_ME__
KIS_APP_SECRET=__FILL_ME__
KIS_ACCOUNT_NUMBER=__FILL_ME__
KIS_BASE_URL=https://openapi.koreainvestment.com:9443
KIS_WS_URL=ws://ops.koreainvestment.com:21000

# Kafka (로컬 Docker compose)
KAFKA_BOOTSTRAP=kafka:9092
KAFKA_TOPIC_TICK=kis.tick.raw

# Symbols (Phase 1 = 3 종목)
KIS_SYMBOLS=005930,000660,035420

# DART (Phase 1B)
DART_API_KEY=__FILL_ME__

# Airflow
AIRFLOW__CORE__DEFAULT_TIMEZONE=Asia/Seoul
AIRFLOW__CORE__EXECUTOR=LocalExecutor

# Trading flag (Phase 2 placeholder, 항상 false)
TRADING_ENABLED=false
```

- [ ] **Step 3: `README.md` 셋업 섹션 추가**

기존 `README.md` 끝에 아래 섹션 append:

```markdown
## Quick Start (Phase 1 dev)

1. `cp .env.example .env` 후 KIS / DART / AWS profile 값 채움
2. AWS 초기 셋업: `bash infra/scripts/aws_initial_setup.sh`
3. Glue/Athena DDL 실행: `bash infra/scripts/run_ddl.sh`
4. Docker 스택 기동: `docker compose -f infra/docker/docker-compose.yml up -d`
5. Airflow UI: http://localhost:8080  (admin/admin)
6. Grafana: http://localhost:3000  (admin/admin)
7. Spark UI: http://localhost:8080  (Spark master 포트 충돌 시 compose에서 변경)

자세한 architecture 는 `docs/superpowers/specs/2026-05-07-tickberg-phase1-mvp-design.md` 참고.
```

- [ ] **Step 4: 검증**

Run: `git status`
Expected: `.gitignore`, `.env.example`, modified `README.md` 출력. `.env` 는 ignore 되어 보이지 않음.

- [ ] **Step 5: Commit**

```bash
git add .gitignore .env.example README.md
git commit -m "chore: project skeleton with .env.example and .gitignore"
```

---

### Task 2: AWS 초기 셋업 스크립트

CLAUDE.md "AWS 비용·자원 가드레일" — S3 버킷 1개, Glue DB 1개, Athena workgroup 1개 (BytesScannedCutoffPerQuery=5GB), IAM 사용자 최소권한, Budgets 월 $20 alarm.

**Files:**
- Create: `infra/scripts/aws_initial_setup.sh`
- Create: `infra/scripts/iam_policy_tickberg.json`
- Create: `infra/scripts/s3_lifecycle_bronze.json`

- [ ] **Step 1: IAM policy 작성 — `infra/scripts/iam_policy_tickberg.json`**

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "S3Bucket",
      "Effect": "Allow",
      "Action": ["s3:ListBucket", "s3:GetBucketLocation"],
      "Resource": "arn:aws:s3:::tickberg-lakehouse"
    },
    {
      "Sid": "S3Object",
      "Effect": "Allow",
      "Action": ["s3:GetObject", "s3:PutObject", "s3:DeleteObject"],
      "Resource": "arn:aws:s3:::tickberg-lakehouse/*"
    },
    {
      "Sid": "Glue",
      "Effect": "Allow",
      "Action": [
        "glue:GetDatabase", "glue:GetDatabases",
        "glue:CreateTable", "glue:GetTable", "glue:GetTables", "glue:UpdateTable", "glue:DeleteTable",
        "glue:GetPartition", "glue:GetPartitions", "glue:CreatePartition", "glue:BatchCreatePartition",
        "glue:UpdatePartition", "glue:DeletePartition", "glue:BatchDeletePartition"
      ],
      "Resource": [
        "arn:aws:glue:ap-northeast-2:*:catalog",
        "arn:aws:glue:ap-northeast-2:*:database/tickberg",
        "arn:aws:glue:ap-northeast-2:*:table/tickberg/*"
      ]
    },
    {
      "Sid": "Athena",
      "Effect": "Allow",
      "Action": [
        "athena:StartQueryExecution", "athena:GetQueryExecution",
        "athena:GetQueryResults", "athena:StopQueryExecution",
        "athena:GetWorkGroup", "athena:ListWorkGroups"
      ],
      "Resource": "arn:aws:athena:ap-northeast-2:*:workgroup/tickberg-wg"
    }
  ]
}
```

- [ ] **Step 2: S3 lifecycle 작성 — `infra/scripts/s3_lifecycle_bronze.json`**

Bronze raw 90일 후 Glacier IR (CLAUDE.md 가드레일).

```json
{
  "Rules": [
    {
      "ID": "bronze-to-glacier-ir",
      "Status": "Enabled",
      "Filter": {"Prefix": "bronze/"},
      "Transitions": [
        {"Days": 90, "StorageClass": "GLACIER_IR"}
      ]
    },
    {
      "ID": "athena-results-expire",
      "Status": "Enabled",
      "Filter": {"Prefix": "athena-results/"},
      "Expiration": {"Days": 30}
    }
  ]
}
```

- [ ] **Step 3: 셋업 스크립트 작성 — `infra/scripts/aws_initial_setup.sh`**

```bash
#!/usr/bin/env bash
set -euo pipefail

: "${AWS_PROFILE:=tickberg}"
: "${AWS_REGION:=ap-northeast-2}"
BUCKET="tickberg-lakehouse"
DB="tickberg"
WG="tickberg-wg"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "==> AWS profile=${AWS_PROFILE} region=${AWS_REGION}"

# 1) S3 bucket
if aws --profile "$AWS_PROFILE" s3api head-bucket --bucket "$BUCKET" 2>/dev/null; then
  echo "[skip] s3://${BUCKET} already exists"
else
  aws --profile "$AWS_PROFILE" s3api create-bucket \
    --bucket "$BUCKET" --region "$AWS_REGION" \
    --create-bucket-configuration LocationConstraint="$AWS_REGION"
  aws --profile "$AWS_PROFILE" s3api put-bucket-versioning \
    --bucket "$BUCKET" --versioning-configuration Status=Enabled
fi

# 2) S3 layout
for prefix in bronze/ silver/ gold/ checkpoints/ athena-results/; do
  aws --profile "$AWS_PROFILE" s3api put-object --bucket "$BUCKET" --key "$prefix" >/dev/null
done

# 3) S3 lifecycle
aws --profile "$AWS_PROFILE" s3api put-bucket-lifecycle-configuration \
  --bucket "$BUCKET" \
  --lifecycle-configuration "file://${SCRIPT_DIR}/s3_lifecycle_bronze.json"

# 4) Glue database
aws --profile "$AWS_PROFILE" glue create-database \
  --database-input "Name=${DB},Description=tickberg phase1 lakehouse" \
  2>/dev/null || echo "[skip] Glue DB ${DB} already exists"

# 5) Athena workgroup with 5GB scan cutoff
aws --profile "$AWS_PROFILE" athena create-work-group \
  --name "$WG" \
  --configuration "ResultConfiguration={OutputLocation=s3://${BUCKET}/athena-results/},EnforceWorkGroupConfiguration=true,PublishCloudWatchMetricsEnabled=true,BytesScannedCutoffPerQuery=5368709120" \
  --description "tickberg phase1 workgroup, 5GB scan limit" \
  2>/dev/null || echo "[skip] Athena workgroup ${WG} already exists"

echo "==> Done. Next: bash infra/scripts/run_ddl.sh"
```

- [ ] **Step 4: chmod + sanity check**

Run:
```bash
chmod +x infra/scripts/aws_initial_setup.sh
bash -n infra/scripts/aws_initial_setup.sh   # syntax check
```
Expected: 종료 코드 0, 출력 없음.

- [ ] **Step 5: 사용자 확인 후 실제 실행 (AWS 비용 발생 가능)**

CLAUDE.md "AWS 비용 발생 가능 작업은 사용자에게 먼저 확인". 사용자 승인 후:
```bash
bash infra/scripts/aws_initial_setup.sh
```
Expected: S3 bucket / Glue DB / Athena WG 생성 또는 `[skip]` 메시지.

- [ ] **Step 6: Commit**

```bash
git add infra/scripts/aws_initial_setup.sh infra/scripts/iam_policy_tickberg.json infra/scripts/s3_lifecycle_bronze.json
git commit -m "feat(infra): AWS initial setup script (S3, Glue DB, Athena workgroup, lifecycle)"
```

---

### Task 3: 로컬 Docker compose 골격

Kafka KRaft + Spark Standalone (master+worker) + Airflow LocalExecutor (Postgres backend) + Prometheus + Grafana.

**Files:**
- Create: `infra/docker/docker-compose.yml`
- Create: `infra/docker/.gitkeep` for `volumes/` (이미 .gitignore 처리)

- [ ] **Step 1: compose 작성 — `infra/docker/docker-compose.yml`**

```yaml
version: "3.9"

x-airflow-common: &airflow-common
  image: apache/airflow:2.9.3-python3.11
  environment: &airflow-env
    AIRFLOW__CORE__EXECUTOR: LocalExecutor
    AIRFLOW__CORE__DEFAULT_TIMEZONE: Asia/Seoul
    AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: postgresql+psycopg2://airflow:airflow@airflow-postgres:5432/airflow
    AIRFLOW__CORE__LOAD_EXAMPLES: "false"
    AIRFLOW__WEBSERVER__EXPOSE_CONFIG: "true"
    AWS_PROFILE: ${AWS_PROFILE:-tickberg}
    AWS_REGION: ${AWS_REGION:-ap-northeast-2}
    S3_BUCKET: ${S3_BUCKET:-tickberg-lakehouse}
  volumes:
    - ../../orchestration/dags:/opt/airflow/dags
    - ../../code:/opt/airflow/code
    - ~/.aws:/home/airflow/.aws:ro
    - airflow_logs:/opt/airflow/logs
  depends_on:
    airflow-postgres:
      condition: service_healthy

services:
  kafka:
    image: bitnami/kafka:3.7
    container_name: tickberg-kafka
    ports: ["9092:9092", "9094:9094"]
    environment:
      KAFKA_CFG_NODE_ID: 1
      KAFKA_CFG_PROCESS_ROLES: broker,controller
      KAFKA_CFG_LISTENERS: PLAINTEXT://:9092,CONTROLLER://:9093,EXTERNAL://:9094
      KAFKA_CFG_ADVERTISED_LISTENERS: PLAINTEXT://kafka:9092,EXTERNAL://localhost:9094
      KAFKA_CFG_LISTENER_SECURITY_PROTOCOL_MAP: PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT,EXTERNAL:PLAINTEXT
      KAFKA_CFG_CONTROLLER_LISTENER_NAMES: CONTROLLER
      KAFKA_CFG_CONTROLLER_QUORUM_VOTERS: 1@kafka:9093
      KAFKA_CFG_INTER_BROKER_LISTENER_NAME: PLAINTEXT
      KAFKA_CFG_AUTO_CREATE_TOPICS_ENABLE: "false"
      KAFKA_CFG_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_CFG_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1
      KAFKA_CFG_LOG_RETENTION_HOURS: 168
      KAFKA_KRAFT_CLUSTER_ID: tickberg-kafka-cluster
    volumes: ["kafka_data:/bitnami/kafka"]
    healthcheck:
      test: ["CMD-SHELL", "kafka-topics.sh --bootstrap-server localhost:9092 --list || exit 1"]
      interval: 10s
      retries: 10

  kafka-jmx-exporter:
    image: bitnami/jmx-exporter:0.20.0
    container_name: tickberg-kafka-jmx
    ports: ["7071:7071"]
    command: ["7071", "/etc/jmx-exporter/kafka.yml"]
    volumes:
      - ../../monitoring/prometheus/jmx_exporter_kafka.yml:/etc/jmx-exporter/kafka.yml:ro
    depends_on: [kafka]

  spark-master:
    build: ./spark
    image: tickberg/spark:3.5.1-iceberg-1.5.2
    container_name: tickberg-spark-master
    environment:
      SPARK_MODE: master
      SPARK_MASTER_HOST: spark-master
      AWS_PROFILE: ${AWS_PROFILE:-tickberg}
      AWS_REGION: ${AWS_REGION:-ap-northeast-2}
    ports: ["7077:7077", "8081:8080"]
    volumes:
      - ../../code:/opt/spark/code
      - ~/.aws:/opt/bitnami/spark/.aws:ro

  spark-worker:
    image: tickberg/spark:3.5.1-iceberg-1.5.2
    container_name: tickberg-spark-worker
    environment:
      SPARK_MODE: worker
      SPARK_MASTER_URL: spark://spark-master:7077
      SPARK_WORKER_MEMORY: 4G
      SPARK_WORKER_CORES: 2
      AWS_PROFILE: ${AWS_PROFILE:-tickberg}
      AWS_REGION: ${AWS_REGION:-ap-northeast-2}
    depends_on: [spark-master]
    volumes:
      - ../../code:/opt/spark/code
      - ~/.aws:/opt/bitnami/spark/.aws:ro

  kafka-ui:
    image: provectuslabs/kafka-ui:v0.7.2
    container_name: tickberg-kafka-ui
    ports: ["8090:8080"]
    environment:
      KAFKA_CLUSTERS_0_NAME: tickberg-local
      KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS: kafka:9092
    depends_on:
      kafka:
        condition: service_healthy

  airflow-postgres:
    image: postgres:15
    container_name: tickberg-airflow-postgres
    environment:
      POSTGRES_USER: airflow
      POSTGRES_PASSWORD: airflow
      POSTGRES_DB: airflow
    volumes: ["airflow_postgres_data:/var/lib/postgresql/data"]
    healthcheck:
      test: ["CMD", "pg_isready", "-U", "airflow"]
      interval: 5s
      retries: 10

  airflow-init:
    <<: *airflow-common
    container_name: tickberg-airflow-init
    entrypoint: /bin/bash
    command:
      - -c
      - |
        airflow db migrate
        airflow users create --username admin --password admin \
          --firstname a --lastname a --role Admin --email a@a.com || true

  airflow-scheduler:
    <<: *airflow-common
    container_name: tickberg-airflow-scheduler
    command: scheduler
    depends_on:
      airflow-init:
        condition: service_completed_successfully

  airflow-webserver:
    <<: *airflow-common
    container_name: tickberg-airflow-web
    command: webserver
    ports: ["8080:8080"]
    depends_on:
      airflow-init:
        condition: service_completed_successfully

  prometheus:
    image: prom/prometheus:v2.52.0
    container_name: tickberg-prometheus
    volumes:
      - ../../monitoring/prometheus/prometheus.yml:/etc/prometheus/prometheus.yml:ro
    ports: ["9090:9090"]

  grafana:
    image: grafana/grafana:10.4.2
    container_name: tickberg-grafana
    environment:
      GF_SECURITY_ADMIN_USER: admin
      GF_SECURITY_ADMIN_PASSWORD: admin
    volumes:
      - ../../monitoring/grafana/provisioning:/etc/grafana/provisioning:ro
      - ../../monitoring/grafana/dashboards:/var/lib/grafana/dashboards:ro
    ports: ["3000:3000"]

volumes:
  kafka_data:
  airflow_postgres_data:
  airflow_logs:

networks:
  default:
    name: tickberg-net
```

- [ ] **Step 2: 검증 (구문)**

Run:
```bash
docker compose -f infra/docker/docker-compose.yml config -q
```
Expected: 종료 코드 0, 출력 없음 (compose schema 통과).

- [ ] **Step 3: Commit**

```bash
git add infra/docker/docker-compose.yml
git commit -m "feat(infra): docker compose with Kafka KRaft, Spark, Airflow, Prom/Grafana"
```

---

### Task 4: Kafka topic 생성 + Prometheus/Grafana 골격

Spec §2.4 — `kis.tick.raw` 12 partitions, RF=1, retention 7d.

**Files:**
- Create: `infra/scripts/kafka_create_topics.sh`
- Create: `monitoring/prometheus/prometheus.yml`
- Create: `monitoring/prometheus/jmx_exporter_kafka.yml`
- Create: `monitoring/grafana/provisioning/datasources/prometheus.yml`
- Create: `monitoring/grafana/provisioning/dashboards/tickberg.yml`
- Create: `monitoring/grafana/dashboards/.gitkeep`

- [ ] **Step 1: Kafka topic 스크립트 — `infra/scripts/kafka_create_topics.sh`**

```bash
#!/usr/bin/env bash
set -euo pipefail
TOPIC="${KAFKA_TOPIC_TICK:-kis.tick.raw}"
PARTITIONS=12
RF=1
RETENTION_MS=$((7*24*60*60*1000))   # 7일

docker exec tickberg-kafka kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create --if-not-exists \
  --topic "$TOPIC" --partitions "$PARTITIONS" --replication-factor "$RF" \
  --config "retention.ms=${RETENTION_MS}" \
  --config "min.insync.replicas=1"

docker exec tickberg-kafka kafka-topics.sh \
  --bootstrap-server localhost:9092 --describe --topic "$TOPIC"
```

- [ ] **Step 2: Prometheus 설정 — `monitoring/prometheus/prometheus.yml`**

```yaml
global:
  scrape_interval: 15s
  evaluation_interval: 15s

scrape_configs:
  - job_name: prometheus
    static_configs: [{targets: ["localhost:9090"]}]

  - job_name: kafka-jmx
    static_configs: [{targets: ["kafka-jmx-exporter:7071"]}]

  - job_name: spark-master
    metrics_path: /metrics/master/prometheus
    static_configs: [{targets: ["spark-master:8080"]}]

  - job_name: spark-driver
    metrics_path: /metrics/prometheus
    static_configs: [{targets: ["host.docker.internal:4040"]}]

  - job_name: kis-producer
    static_configs: [{targets: ["kis-producer:9100"]}]
```

- [ ] **Step 3: JMX exporter for Kafka — `monitoring/prometheus/jmx_exporter_kafka.yml`**

```yaml
hostPort: kafka:9999
ssl: false
lowercaseOutputName: true
rules:
  - pattern: kafka.consumer<type=consumer-fetch-manager-metrics, client-id=(.+), topic=(.+), partition=(.+)><>(.+):
    name: kafka_consumer_$4
    labels:
      client_id: "$1"
      topic: "$2"
      partition: "$3"
  - pattern: kafka.controller<type=KafkaController, name=(.+)><>Value
    name: kafka_controller_$1
  - pattern: kafka.server<type=BrokerTopicMetrics, name=(.+), topic=(.+)><>OneMinuteRate
    name: kafka_topic_$1_rate
    labels:
      topic: "$2"
  - pattern: kafka.consumergroup<type=GroupMetadataManager, group=(.+), topic=(.+), partition=(.+)><>Value
    name: kafka_consumergroup_lag
    labels:
      group: "$1"
      topic: "$2"
      partition: "$3"
```

- [ ] **Step 4: Grafana datasource provisioning — `monitoring/grafana/provisioning/datasources/prometheus.yml`**

```yaml
apiVersion: 1
datasources:
  - name: Prometheus
    type: prometheus
    access: proxy
    url: http://prometheus:9090
    isDefault: true
```

- [ ] **Step 5: Grafana dashboard provider — `monitoring/grafana/provisioning/dashboards/tickberg.yml`**

```yaml
apiVersion: 1
providers:
  - name: tickberg
    orgId: 1
    folder: ""
    type: file
    disableDeletion: false
    updateIntervalSeconds: 30
    options:
      path: /var/lib/grafana/dashboards
```

- [ ] **Step 6: 검증 — compose up + topic 생성**

```bash
docker compose -f infra/docker/docker-compose.yml up -d kafka airflow-postgres airflow-init airflow-scheduler airflow-webserver prometheus grafana spark-master spark-worker
sleep 20
chmod +x infra/scripts/kafka_create_topics.sh
bash infra/scripts/kafka_create_topics.sh
```
Expected: `Created topic kis.tick.raw.` + describe 출력에서 `PartitionCount: 12, ReplicationFactor: 1`.

- [ ] **Step 7: Commit**

```bash
git add infra/scripts/kafka_create_topics.sh monitoring/prometheus/ monitoring/grafana/provisioning/ monitoring/grafana/dashboards/.gitkeep
git commit -m "feat(infra): Kafka topic creation script and Prom/Grafana provisioning"
```

---

### Task 5: Bronze DDL — `bronze.kis_tick_raw` (Parquet external + Glue partition projection)

Spec §3.1.

**Files:**
- Create: `code/ddl/bronze/kis_tick_raw.sql`
- Create: `infra/scripts/run_ddl.sh`

- [ ] **Step 1: DDL 작성 — `code/ddl/bronze/kis_tick_raw.sql`**

```sql
CREATE EXTERNAL TABLE IF NOT EXISTS tickberg.bronze_kis_tick_raw (
  ingest_ts          timestamp,
  kafka_partition    int,
  kafka_offset       bigint,
  symbol             string,
  trade_ts_kst       timestamp,
  price              decimal(18,2),
  volume             bigint,
  cum_volume         bigint,
  cum_amount         bigint,
  trade_side         string,
  best_ask_price     decimal(18,2),
  best_bid_price     decimal(18,2),
  raw_payload        string
)
PARTITIONED BY (dt date, hr int)
STORED AS PARQUET
LOCATION 's3://tickberg-lakehouse/bronze/kis_tick_raw/'
TBLPROPERTIES (
  'projection.enabled'                = 'true',
  'projection.dt.type'                = 'date',
  'projection.dt.range'               = '2026-05-01,NOW',
  'projection.dt.format'              = 'yyyy-MM-dd',
  'projection.dt.interval'            = '1',
  'projection.dt.interval.unit'       = 'DAYS',
  'projection.hr.type'                = 'integer',
  'projection.hr.range'               = '0,23',
  'storage.location.template'         = 's3://tickberg-lakehouse/bronze/kis_tick_raw/dt=${dt}/hr=${hr}/'
);
```

- [ ] **Step 2: DDL 실행 helper — `infra/scripts/run_ddl.sh`**

```bash
#!/usr/bin/env bash
set -euo pipefail
: "${AWS_PROFILE:=tickberg}"
: "${AWS_REGION:=ap-northeast-2}"
WG="tickberg-wg"
DB="tickberg"
RESULTS="s3://tickberg-lakehouse/athena-results/"

run_sql() {
  local file="$1"
  echo "==> $file"
  local qid
  qid=$(aws --profile "$AWS_PROFILE" --region "$AWS_REGION" athena start-query-execution \
    --query-string "$(cat "$file")" \
    --query-execution-context "Database=${DB}" \
    --work-group "$WG" \
    --result-configuration "OutputLocation=${RESULTS}" \
    --output text --query 'QueryExecutionId')
  while :; do
    state=$(aws --profile "$AWS_PROFILE" --region "$AWS_REGION" athena get-query-execution \
      --query-execution-id "$qid" --output text --query 'QueryExecution.Status.State')
    case "$state" in
      SUCCEEDED) echo "  OK ($qid)"; break;;
      FAILED|CANCELLED)
        reason=$(aws --profile "$AWS_PROFILE" --region "$AWS_REGION" athena get-query-execution \
          --query-execution-id "$qid" --output text --query 'QueryExecution.Status.StateChangeReason')
        echo "  FAILED: $reason" >&2; exit 1;;
      *) sleep 2;;
    esac
  done
}

# Bronze (Glue 일반 테이블 — Athena 로 실행)
run_sql code/ddl/bronze/kis_tick_raw.sql

# Silver/Gold (Iceberg — Athena Iceberg 지원)
for f in code/ddl/silver/*.sql code/ddl/gold/*.sql; do
  [ -f "$f" ] && run_sql "$f"
done

echo "All DDL applied."
```

- [ ] **Step 3: Bronze DDL 만 일단 실행 (사용자 승인 후)**

```bash
chmod +x infra/scripts/run_ddl.sh
bash infra/scripts/run_ddl.sh
```
Expected: Bronze DDL `OK (<query-id>)`. Silver/Gold 는 다음 Task에서 추가.

- [ ] **Step 4: Athena 콘솔 또는 CLI 로 검증**

```bash
aws --profile tickberg athena start-query-execution \
  --query-string "SHOW TABLES IN tickberg" \
  --work-group tickberg-wg \
  --result-configuration "OutputLocation=s3://tickberg-lakehouse/athena-results/"
```
Expected: 결과에 `bronze_kis_tick_raw` 포함.

- [ ] **Step 5: Commit**

```bash
git add code/ddl/bronze/kis_tick_raw.sql infra/scripts/run_ddl.sh
git commit -m "feat(ddl): bronze.kis_tick_raw external table with Glue partition projection"
```

---

### Task 6: Silver DDL — `silver.kis_tick_clean` + `silver.dim_symbol` (Iceberg v2)

Spec §3.2, §3.3.

**Files:**
- Create: `code/ddl/silver/kis_tick_clean.sql`
- Create: `code/ddl/silver/dim_symbol.sql`

- [ ] **Step 1: `code/ddl/silver/kis_tick_clean.sql`**

```sql
CREATE TABLE IF NOT EXISTS tickberg.silver_kis_tick_clean (
  trade_uid       string,
  symbol          string,
  trade_ts_kst    timestamp,
  trade_ts_utc    timestamp,
  price           decimal(18,2),
  volume          bigint,
  trade_amount    decimal(20,2),
  trade_side      string,
  best_ask_price  decimal(18,2),
  best_bid_price  decimal(18,2),
  ingest_ts       timestamp,
  silver_ts       timestamp
)
PARTITIONED BY (day(trade_ts_kst), hour(trade_ts_kst))
LOCATION 's3://tickberg-lakehouse/silver/kis_tick_clean/'
TBLPROPERTIES (
  'table_type'                       = 'ICEBERG',
  'format'                           = 'parquet',
  'format-version'                   = '2',
  'write.distribution-mode'          = 'hash',
  'write.target-file-size-bytes'     = '268435456',
  'write.delete.mode'                = 'merge-on-read',
  'write.update.mode'                = 'merge-on-read',
  'write.merge.mode'                 = 'merge-on-read'
);
```

- [ ] **Step 2: `code/ddl/silver/dim_symbol.sql`**

```sql
CREATE TABLE IF NOT EXISTS tickberg.silver_dim_symbol (
  symbol             string,
  symbol_name        string,
  market             string,
  par_value          decimal(18,2),
  shares_outstanding bigint,
  is_active          boolean,
  updated_ts         timestamp
)
PARTITIONED BY (market)
LOCATION 's3://tickberg-lakehouse/silver/dim_symbol/'
TBLPROPERTIES (
  'table_type'      = 'ICEBERG',
  'format'          = 'parquet',
  'format-version'  = '2'
);
```

- [ ] **Step 3: 실행 (사용자 승인 후)**

```bash
bash infra/scripts/run_ddl.sh
```
Expected: Bronze (skip — 이미 존재) + Silver 2개 OK.

- [ ] **Step 4: Commit**

```bash
git add code/ddl/silver/
git commit -m "feat(ddl): silver Iceberg v2 — kis_tick_clean + dim_symbol"
```

---

### Task 7: Gold DDL — `gold.symbol_vwap_1m` (Iceberg)

Spec §3.4.

**Files:**
- Create: `code/ddl/gold/symbol_vwap_1m.sql`

- [ ] **Step 1: `code/ddl/gold/symbol_vwap_1m.sql`**

```sql
CREATE TABLE IF NOT EXISTS tickberg.gold_symbol_vwap_1m (
  symbol         string,
  ts_minute      timestamp,
  open_price     decimal(18,2),
  close_price    decimal(18,2),
  high_price     decimal(18,2),
  low_price      decimal(18,2),
  total_volume   bigint,
  vwap           decimal(18,4),
  trade_count    int,
  computed_at    timestamp
)
PARTITIONED BY (day(ts_minute), hour(ts_minute))
LOCATION 's3://tickberg-lakehouse/gold/symbol_vwap_1m/'
TBLPROPERTIES (
  'table_type'                   = 'ICEBERG',
  'format'                       = 'parquet',
  'format-version'               = '2',
  'write.target-file-size-bytes' = '268435456'
);
```

- [ ] **Step 2: 실행 + Commit**

```bash
bash infra/scripts/run_ddl.sh
git add code/ddl/gold/
git commit -m "feat(ddl): gold.symbol_vwap_1m Iceberg with hour partition"
```

---

### Task 8: KIS payload parser (TDD)

Spec §3.1, §7.3 — H0STCNT0 raw text → dict.  
KIS H0STCNT0 frame format (체결 데이터): `|`-구분 텍스트. 각 필드 의미는 KIS API 문서 H0STCNT0 spec 기준.

**Files:**
- Create: `infra/docker/kis-producer/src/__init__.py`
- Create: `infra/docker/kis-producer/src/parser.py`
- Create: `tests/__init__.py`
- Create: `tests/conftest.py`
- Create: `tests/fixtures/kis_h0stcnt0_samples.py`
- Create: `tests/test_kis_parser.py`
- Create: `pyproject.toml`

- [ ] **Step 1: `pyproject.toml` 작성 (의존성·pytest 설정)**

```toml
[project]
name = "tickberg"
version = "0.1.0"
requires-python = ">=3.11"

[tool.pytest.ini_options]
testpaths = ["tests"]
pythonpath = ["infra/docker/kis-producer", "code"]
addopts = "-ra -q"

[tool.ruff]
line-length = 100
target-version = "py311"

[tool.ruff.lint]
select = ["E", "F", "I", "B", "UP"]

[tool.black]
line-length = 100
target-version = ["py311"]
```

- [ ] **Step 2: fixture — `tests/fixtures/kis_h0stcnt0_samples.py`**

H0STCNT0 framing reference: 1번째 필드 = MKSC_SHRN_ISCD (symbol), 2번째 = STCK_CNTG_HOUR (HHMMSS), 3번째 = STCK_PRPR (price), … 여기서는 spec 와 일치하는 13개 필드만 사용.

```python
SAMPLE_NORMAL = (
    "005930|093001|72500|+|100|0|72500|72400|72600|72400|"
    "1234567|89500000000|1"
)
SAMPLE_NEGATIVE_PRICE = (
    "005930|093002|-100|-|50|0|-100|72400|72600|72400|"
    "1234617|89500050000|2"
)
SAMPLE_MISSING_TRADE_SIDE = (
    "000660|093003|125000||30|0|125000|124900|125100|124900|"
    "456789|56000000000|"
)
SAMPLE_NULL_PADDED = (
    "035420|093004|185500|+|10|0|185500|185400|185600|185400|"
    "111222|20600000000|1"
)
SAMPLE_MULTI_LINE = "\n".join([SAMPLE_NORMAL, SAMPLE_NULL_PADDED])
```

- [ ] **Step 3: 실패 테스트 작성 — `tests/test_kis_parser.py`**

```python
from datetime import date
from decimal import Decimal

import pytest

from src.parser import ParseError, parse_h0stcnt0
from tests.fixtures.kis_h0stcnt0_samples import (
    SAMPLE_MISSING_TRADE_SIDE, SAMPLE_MULTI_LINE, SAMPLE_NEGATIVE_PRICE,
    SAMPLE_NORMAL, SAMPLE_NULL_PADDED,
)


def test_parses_basic_fields():
    rows = parse_h0stcnt0(SAMPLE_NORMAL, business_day=date(2026, 5, 8))
    assert len(rows) == 1
    r = rows[0]
    assert r["symbol"] == "005930"
    assert r["price"] == Decimal("72500")
    assert r["volume"] == 100
    assert r["cum_volume"] == 1234567
    assert r["cum_amount"] == 89500000000
    assert r["best_ask_price"] == Decimal("72500")
    assert r["best_bid_price"] == Decimal("72400")
    assert r["trade_ts_kst"].isoformat() == "2026-05-08T09:30:01"
    assert r["trade_side"] == "+"


def test_negative_price_rejected():
    with pytest.raises(ParseError):
        parse_h0stcnt0(SAMPLE_NEGATIVE_PRICE, business_day=date(2026, 5, 8))


def test_null_trade_side_normalized_to_none():
    rows = parse_h0stcnt0(SAMPLE_MISSING_TRADE_SIDE, business_day=date(2026, 5, 8))
    assert rows[0]["trade_side"] is None


def test_multiline_payload_yields_multiple_rows():
    rows = parse_h0stcnt0(SAMPLE_MULTI_LINE, business_day=date(2026, 5, 8))
    assert len(rows) == 2
    assert {r["symbol"] for r in rows} == {"005930", "035420"}


def test_raw_payload_preserved():
    rows = parse_h0stcnt0(SAMPLE_NULL_PADDED, business_day=date(2026, 5, 8))
    assert rows[0]["raw_payload"] == SAMPLE_NULL_PADDED
```

- [ ] **Step 4: Run failing tests**

```bash
pip install pytest
pytest tests/test_kis_parser.py -v
```
Expected: 5 errors with `ModuleNotFoundError: No module named 'src.parser'`.

- [ ] **Step 5: Implement parser — `infra/docker/kis-producer/src/parser.py`**

```python
"""KIS H0STCNT0 (체결가) payload parser.

H0STCNT0 frame은 '|'-구분 텍스트. 한 frame에 여러 줄(레코드) 가능.
1차 발표 13 필드: 0=symbol(MKSC_SHRN_ISCD), 1=trade_time(HHMMSS),
2=price(STCK_PRPR), 3=trade_side(CCLD_DVSN: '+' buy / '-' sell / '' unknown),
4=volume(CNTG_VOL), 5=...spread, 6..9=ASKP1/BIDP1/BIDP2/BIDP3 등 (broker-specific),
10=cum_volume(ACML_VOL), 11=cum_amount(ACML_TR_PBMN), 12=trade_ratio.
운영 중 KIS docs와 mismatch 발견하면 fixture에 변종 추가하고 보강.
"""
from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from typing import Any


class ParseError(ValueError):
    """KIS payload parse 실패. raw text 보존하고 caller가 dead-letter 처리."""


_REQUIRED_FIELDS = 13


def _parse_decimal_positive(s: str, field: str) -> Decimal:
    try:
        v = Decimal(s)
    except InvalidOperation as e:
        raise ParseError(f"{field} not decimal: {s!r}") from e
    if v < 0:
        raise ParseError(f"{field} negative: {v}")
    return v


def _parse_int_nonneg(s: str, field: str) -> int:
    try:
        v = int(s)
    except ValueError as e:
        raise ParseError(f"{field} not int: {s!r}") from e
    if v < 0:
        raise ParseError(f"{field} negative: {v}")
    return v


def _parse_line(line: str, business_day: date) -> dict[str, Any]:
    parts = line.split("|")
    if len(parts) < _REQUIRED_FIELDS:
        raise ParseError(f"need >= {_REQUIRED_FIELDS} fields, got {len(parts)}")

    symbol = parts[0]
    hhmmss = parts[1]
    if len(hhmmss) != 6 or not hhmmss.isdigit():
        raise ParseError(f"trade_time invalid: {hhmmss!r}")

    trade_dt = datetime(
        business_day.year, business_day.month, business_day.day,
        int(hhmmss[0:2]), int(hhmmss[2:4]), int(hhmmss[4:6]),
    )
    side = parts[3] or None  # '' → None

    return {
        "symbol": symbol,
        "trade_ts_kst": trade_dt,
        "price": _parse_decimal_positive(parts[2], "price"),
        "trade_side": side,
        "volume": _parse_int_nonneg(parts[4], "volume"),
        "best_ask_price": _parse_decimal_positive(parts[6], "best_ask_price"),
        "best_bid_price": _parse_decimal_positive(parts[7], "best_bid_price"),
        "cum_volume": _parse_int_nonneg(parts[10], "cum_volume"),
        "cum_amount": _parse_int_nonneg(parts[11], "cum_amount"),
        "raw_payload": line,
    }


def parse_h0stcnt0(payload: str, *, business_day: date) -> list[dict[str, Any]]:
    """KIS H0STCNT0 frame → list of dict rows.

    Single frame may carry multiple newline-separated records.
    """
    rows: list[dict[str, Any]] = []
    for raw_line in payload.splitlines():
        line = raw_line.strip()
        if not line:
            continue
        rows.append(_parse_line(line, business_day))
    return rows
```

- [ ] **Step 6: `tests/__init__.py`, `tests/conftest.py` 빈 파일 + `tests/fixtures/__init__.py`**

```python
# tests/__init__.py — empty
# tests/conftest.py — empty (future fixtures here)
# tests/fixtures/__init__.py — empty
```

- [ ] **Step 7: Run tests, verify pass**

```bash
pytest tests/test_kis_parser.py -v
```
Expected: 5 passed.

- [ ] **Step 8: Commit**

```bash
git add pyproject.toml infra/docker/kis-producer/src/parser.py infra/docker/kis-producer/src/__init__.py tests/
git commit -m "feat(producer): KIS H0STCNT0 parser with edge-case unit tests"
```

---

### Task 9: KIS OAuth client + access_token store

Spec §6.1.1 — 03:30 KST 고정 refresh, approval_key 동시 갱신, 만료 추적 로직 불필요.

**Files:**
- Create: `infra/docker/kis-producer/src/kis_auth.py`
- Create: `tests/test_kis_auth.py`

- [ ] **Step 1: 실패 테스트 작성 — `tests/test_kis_auth.py`**

```python
from unittest.mock import AsyncMock

import pytest

from src.kis_auth import KisAuth


@pytest.mark.asyncio
async def test_token_refresh_calls_oauth_then_websocket_key():
    rest_mock = AsyncMock()
    rest_mock.post = AsyncMock(side_effect=[
        {"access_token": "ACC", "expires_in": 86400, "token_type": "Bearer"},
        {"approval_key": "APP"},
    ])
    auth = KisAuth(
        app_key="K", app_secret="S",
        base_url="https://kis", http=rest_mock,
    )

    await auth.refresh()

    assert auth.access_token == "ACC"
    assert auth.approval_key == "APP"
    assert rest_mock.post.await_count == 2


@pytest.mark.asyncio
async def test_refresh_failure_keeps_previous_token():
    rest_mock = AsyncMock()
    auth = KisAuth(app_key="K", app_secret="S", base_url="https://kis", http=rest_mock)
    auth._access_token = "OLD"  # type: ignore[attr-defined]
    auth._approval_key = "OLDKEY"  # type: ignore[attr-defined]
    rest_mock.post = AsyncMock(side_effect=RuntimeError("network"))

    with pytest.raises(RuntimeError):
        await auth.refresh()

    assert auth.access_token == "OLD"
    assert auth.approval_key == "OLDKEY"
```

- [ ] **Step 2: install pytest-asyncio + run failing**

```bash
pip install pytest-asyncio
pytest tests/test_kis_auth.py -v
```
Add to `pyproject.toml` under `[tool.pytest.ini_options]`: `asyncio_mode = "auto"`.

Expected: 2 errors (`ModuleNotFoundError: No module named 'src.kis_auth'`).

- [ ] **Step 3: Implement — `infra/docker/kis-producer/src/kis_auth.py`**

```python
"""KIS OAuth — REST access_token + WebSocket approval_key.

만료 추적 로직 의도적으로 없음. KisAuth.refresh() 는 매일 03:30 KST 외부
스케줄러 (token_refresher.py) 가 호출. 첫 가동 시 (start-up) 도 1회 호출.
"""
from __future__ import annotations

from typing import Protocol


class _HttpClient(Protocol):
    async def post(self, path: str, *, json: dict, headers: dict | None = ...) -> dict: ...


class KisAuth:
    def __init__(self, *, app_key: str, app_secret: str, base_url: str, http: _HttpClient):
        self._app_key = app_key
        self._app_secret = app_secret
        self._base_url = base_url.rstrip("/")
        self._http = http
        self._access_token: str | None = None
        self._approval_key: str | None = None

    @property
    def access_token(self) -> str | None:
        return self._access_token

    @property
    def approval_key(self) -> str | None:
        return self._approval_key

    async def refresh(self) -> None:
        """Refresh access_token and approval_key atomically.

        실패 시 기존 토큰 유지 (caller 가 retry 또는 alert 결정).
        """
        token_resp = await self._http.post(
            "/oauth2/tokenP",
            json={
                "grant_type": "client_credentials",
                "appkey": self._app_key,
                "appsecret": self._app_secret,
            },
            headers={"content-type": "application/json"},
        )
        new_token = token_resp["access_token"]

        approval_resp = await self._http.post(
            "/oauth2/Approval",
            json={
                "grant_type": "client_credentials",
                "appkey": self._app_key,
                "secretkey": self._app_secret,
            },
            headers={"content-type": "application/json"},
        )
        new_approval = approval_resp["approval_key"]

        self._access_token = new_token
        self._approval_key = new_approval
```

- [ ] **Step 4: Run tests**

```bash
pytest tests/test_kis_auth.py -v
```
Expected: 2 passed.

- [ ] **Step 5: Commit**

```bash
git add infra/docker/kis-producer/src/kis_auth.py tests/test_kis_auth.py pyproject.toml
git commit -m "feat(producer): KIS OAuth client with atomic token+approval-key refresh"
```

---

### Task 10: KIS WebSocket subscription with reconnect / heartbeat

Spec §6.1 — exponential backoff 재접속 (1s→60s cap), 60s heartbeat timeout, 메모리상 종목 list 자동 re-subscribe.

**Files:**
- Create: `infra/docker/kis-producer/src/kis_websocket.py`
- Create: `tests/test_kis_websocket.py`

- [ ] **Step 1: 실패 테스트 작성 — `tests/test_kis_websocket.py`**

```python
import pytest

from src.kis_websocket import _next_backoff_seconds


@pytest.mark.parametrize(
    "attempt, expected",
    [(0, 1), (1, 2), (2, 4), (3, 8), (4, 16), (5, 32), (6, 60), (10, 60)],
)
def test_backoff_caps_at_60(attempt, expected):
    assert _next_backoff_seconds(attempt) == expected
```

- [ ] **Step 2: Run failing**

```bash
pytest tests/test_kis_websocket.py -v
```
Expected: ImportError.

- [ ] **Step 3: Implement — `infra/docker/kis-producer/src/kis_websocket.py`**

```python
"""KIS WebSocket subscription with reconnect.

영업시간 외 (장 마감 후) 메시지 0 도 정상이라 ws_connected 만 가지고 alert 하지 않음.
호출자 (main.py) 가 영업시간 컨텍스트와 결합해서 판단."""
from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import AsyncIterator, Awaitable, Callable
from datetime import date

from src.kis_auth import KisAuth
from src.parser import ParseError, parse_h0stcnt0

log = logging.getLogger("kis_websocket")

_HEARTBEAT_TIMEOUT_S = 60
_BACKOFF_CAP_S = 60


def _next_backoff_seconds(attempt: int) -> int:
    """1, 2, 4, 8, 16, 32, 60, 60, ... (cap = 60s)."""
    return min(2**attempt, _BACKOFF_CAP_S)


def _build_subscribe_frame(approval_key: str, symbol: str) -> str:
    return json.dumps({
        "header": {
            "approval_key": approval_key,
            "custtype": "P",
            "tr_type": "1",
            "content-type": "utf-8",
        },
        "body": {"input": {"tr_id": "H0STCNT0", "tr_key": symbol}},
    })


class KisWebSocket:
    def __init__(
        self, *,
        ws_url: str,
        auth: KisAuth,
        symbols: list[str],
        ws_connect: Callable[[str], Awaitable["_WsLike"]],
        on_connect: Callable[[bool], None] | None = None,
        business_day_provider: Callable[[], date] = lambda: date.today(),
    ):
        self._ws_url = ws_url
        self._auth = auth
        self._symbols = list(symbols)
        self._ws_connect = ws_connect
        self._on_connect = on_connect or (lambda _b: None)
        self._business_day = business_day_provider

    async def stream(self) -> AsyncIterator[dict]:
        attempt = 0
        while True:
            try:
                async for parsed in self._connect_and_consume():
                    attempt = 0
                    yield parsed
            except asyncio.CancelledError:
                raise
            except Exception as e:  # noqa: BLE001
                self._on_connect(False)
                wait = _next_backoff_seconds(attempt)
                log.warning("ws disconnected (%s). reconnect in %ds", e, wait)
                await asyncio.sleep(wait)
                attempt += 1

    async def _connect_and_consume(self) -> AsyncIterator[dict]:
        if not self._auth.approval_key:
            raise RuntimeError("approval_key missing — call KisAuth.refresh() first")
        ws = await self._ws_connect(self._ws_url)
        try:
            self._on_connect(True)
            for sym in self._symbols:
                await ws.send(_build_subscribe_frame(self._auth.approval_key, sym))
            log.info("subscribed %d symbols", len(self._symbols))

            while True:
                msg = await asyncio.wait_for(ws.recv(), timeout=_HEARTBEAT_TIMEOUT_S)
                if isinstance(msg, str) and msg.startswith("{"):
                    # control frame (subscribe ack / heartbeat). skip.
                    continue
                payload = msg if isinstance(msg, str) else msg.decode("utf-8")
                try:
                    rows = parse_h0stcnt0(payload, business_day=self._business_day())
                except ParseError as e:
                    log.error("parse error: %s | raw=%s", e, payload[:200])
                    continue
                for r in rows:
                    yield r
        finally:
            await ws.close()


class _WsLike:  # pragma: no cover — structural only
    async def send(self, data: str) -> None: ...
    async def recv(self) -> str | bytes: ...
    async def close(self) -> None: ...
```

- [ ] **Step 4: Run + commit**

```bash
pytest tests/test_kis_websocket.py -v
```
Expected: 8 passed.

```bash
git add infra/docker/kis-producer/src/kis_websocket.py tests/test_kis_websocket.py
git commit -m "feat(producer): KIS WebSocket subscriber with backoff and heartbeat timeout"
```

---

### Task 11: Kafka publisher (idempotent, symbol partition key)

Spec §2.4 (key=symbol), §6.2 (`acks=all`, idempotent, retries=10, snappy).

**Files:**
- Create: `infra/docker/kis-producer/src/kafka_publisher.py`
- Create: `tests/test_kafka_publisher.py`

- [ ] **Step 1: 실패 테스트 — `tests/test_kafka_publisher.py`**

```python
import json
from datetime import datetime
from decimal import Decimal
from unittest.mock import AsyncMock

import pytest

from src.kafka_publisher import KafkaTickPublisher


@pytest.mark.asyncio
async def test_publish_uses_symbol_as_partition_key_and_serializes_decimals():
    sent = []
    sender = AsyncMock(side_effect=lambda topic, value, key: sent.append((topic, value, key)))
    pub = KafkaTickPublisher(topic="kis.tick.raw", send=sender)

    await pub.publish({
        "symbol": "005930",
        "trade_ts_kst": datetime(2026, 5, 8, 9, 30, 1),
        "price": Decimal("72500"),
        "volume": 100,
        "trade_side": "+",
        "best_ask_price": Decimal("72500"),
        "best_bid_price": Decimal("72400"),
        "cum_volume": 1234567,
        "cum_amount": 89500000000,
        "raw_payload": "005930|093001|72500|...",
    })

    assert sent[0][0] == "kis.tick.raw"
    assert sent[0][2] == b"005930"
    decoded = json.loads(sent[0][1])
    assert decoded["price"] == "72500"
    assert decoded["trade_ts_kst"] == "2026-05-08T09:30:01"
```

- [ ] **Step 2: Implement — `infra/docker/kis-producer/src/kafka_publisher.py`**

```python
"""Kafka publisher — symbol partition key + JSON serialize.

aiokafka 의 producer config (acks=all, enable_idempotence=True, retries=10,
linger_ms=50, compression_type=snappy) 는 main.py 에서 주입.
"""
from __future__ import annotations

import json
from collections.abc import Awaitable, Callable
from datetime import datetime
from decimal import Decimal
from typing import Any

_Sender = Callable[..., Awaitable[Any]]  # send(topic, value, key)


def _default(v: Any) -> Any:
    if isinstance(v, Decimal):
        return str(v)
    if isinstance(v, datetime):
        return v.isoformat(timespec="seconds")
    raise TypeError(f"unserialized type: {type(v)}")


class KafkaTickPublisher:
    def __init__(self, *, topic: str, send: _Sender):
        self._topic = topic
        self._send = send

    async def publish(self, row: dict[str, Any]) -> None:
        symbol = row["symbol"]
        if not isinstance(symbol, str) or not symbol:
            raise ValueError("symbol required")
        payload = json.dumps(row, default=_default, ensure_ascii=False).encode("utf-8")
        await self._send(self._topic, payload, symbol.encode("utf-8"))
```

- [ ] **Step 3: Run + commit**

```bash
pytest tests/test_kafka_publisher.py -v
git add infra/docker/kis-producer/src/kafka_publisher.py tests/test_kafka_publisher.py
git commit -m "feat(producer): Kafka publisher with symbol partition key and JSON serialization"
```

---

### Task 12: 03:30 KST token refresher + Prometheus health metrics

Spec §6.1.1, §5.3.

**Files:**
- Create: `infra/docker/kis-producer/src/token_refresher.py`
- Create: `infra/docker/kis-producer/src/health_metrics.py`
- Create: `tests/test_token_refresher.py`

- [ ] **Step 1: 실패 테스트 — `tests/test_token_refresher.py`**

```python
from datetime import datetime
from zoneinfo import ZoneInfo

from src.token_refresher import next_refresh_at

KST = ZoneInfo("Asia/Seoul")


def test_next_refresh_today_when_before_0330():
    now = datetime(2026, 5, 8, 3, 0, tzinfo=KST)
    assert next_refresh_at(now).isoformat() == "2026-05-08T03:30:00+09:00"


def test_next_refresh_tomorrow_when_after_0330():
    now = datetime(2026, 5, 8, 3, 31, tzinfo=KST)
    assert next_refresh_at(now).isoformat() == "2026-05-09T03:30:00+09:00"


def test_exactly_0330_picks_tomorrow():
    now = datetime(2026, 5, 8, 3, 30, tzinfo=KST)
    assert next_refresh_at(now).isoformat() == "2026-05-09T03:30:00+09:00"
```

- [ ] **Step 2: Implement — `infra/docker/kis-producer/src/token_refresher.py`**

```python
"""Daily 03:30 KST token refresh.

만료시간 추적 안 함 (spec §6.1.1, D15). 매일 03:30 강제 교체.
04:30 cutoff 이후에도 실패 시 critical alert (caller 가 관리).
"""
from __future__ import annotations

import asyncio
import logging
from datetime import datetime, time, timedelta
from zoneinfo import ZoneInfo

from src.kis_auth import KisAuth

log = logging.getLogger("token_refresher")
KST = ZoneInfo("Asia/Seoul")
_REFRESH_TIME = time(3, 30)
_RETRY_BACKOFF_S = 5 * 60   # 5분 간격
_CUTOFF_TIME = time(4, 30)


def next_refresh_at(now: datetime) -> datetime:
    today_target = now.replace(
        hour=_REFRESH_TIME.hour, minute=_REFRESH_TIME.minute,
        second=0, microsecond=0,
    )
    if now < today_target:
        return today_target
    return today_target + timedelta(days=1)


async def run(auth: KisAuth, on_failure: callable | None = None) -> None:
    while True:
        now = datetime.now(KST)
        target = next_refresh_at(now)
        sleep_s = (target - now).total_seconds()
        log.info("next refresh at %s (sleep %.0fs)", target.isoformat(), sleep_s)
        await asyncio.sleep(sleep_s)

        success = False
        while not success:
            try:
                await auth.refresh()
                success = True
                log.info("token refreshed")
            except Exception as e:  # noqa: BLE001
                if on_failure:
                    on_failure(e)
                if datetime.now(KST).time() > _CUTOFF_TIME:
                    log.critical("refresh failed past 04:30 cutoff: %s", e)
                    break
                log.warning("refresh failed, retry in %ds: %s", _RETRY_BACKOFF_S, e)
                await asyncio.sleep(_RETRY_BACKOFF_S)
```

- [ ] **Step 3: Implement — `infra/docker/kis-producer/src/health_metrics.py`**

```python
"""Prometheus metrics for KIS producer (T1 Grafana panels).

Exposes:
  kis_ws_connected{}            gauge 0/1
  kis_messages_published_total  counter
  kis_parse_errors_total        counter
  kis_token_refresh_failures_total counter
"""
from __future__ import annotations

from prometheus_client import Counter, Gauge, start_http_server

ws_connected = Gauge("kis_ws_connected", "1 if KIS WebSocket connected, else 0")
messages_published = Counter("kis_messages_published_total", "Tick messages published to Kafka", ["symbol"])
parse_errors = Counter("kis_parse_errors_total", "KIS payload parse errors")
token_refresh_failures = Counter("kis_token_refresh_failures_total", "Token refresh failures")


def start(port: int = 9100) -> None:
    start_http_server(port)
```

- [ ] **Step 4: Run + commit**

```bash
pytest tests/test_token_refresher.py -v
git add infra/docker/kis-producer/src/token_refresher.py infra/docker/kis-producer/src/health_metrics.py tests/test_token_refresher.py
git commit -m "feat(producer): 03:30 KST token refresher and Prometheus health metrics"
```

---

### Task 13: Producer entrypoint (`main.py`) — wires everything

**Files:**
- Create: `infra/docker/kis-producer/main.py`
- Create: `infra/docker/kis-producer/requirements.txt`

- [ ] **Step 1: `infra/docker/kis-producer/requirements.txt`**

```
aiohttp==3.9.5
aiokafka==0.10.0
websockets==12.0
prometheus-client==0.20.0
python-dotenv==1.0.1
```

- [ ] **Step 2: `infra/docker/kis-producer/main.py`**

```python
"""KIS producer entrypoint.

장 시간대 (영업일 09:00–15:30 KST) 만 메시지 흐름 발생.
장 마감 후 메시지 0 = 정상 (Grafana ws_connected 만 alert 대상).
"""
from __future__ import annotations

import asyncio
import logging
import os
from datetime import date, datetime
from zoneinfo import ZoneInfo

import aiohttp
from aiokafka import AIOKafkaProducer
import websockets

from src import health_metrics, token_refresher
from src.kafka_publisher import KafkaTickPublisher
from src.kis_auth import KisAuth
from src.kis_websocket import KisWebSocket

KST = ZoneInfo("Asia/Seoul")
log = logging.getLogger("kis_producer")


class _AiohttpKisHttp:
    def __init__(self, base_url: str, session: aiohttp.ClientSession):
        self._base_url = base_url.rstrip("/")
        self._session = session

    async def post(self, path: str, *, json: dict, headers: dict | None = None) -> dict:
        async with self._session.post(self._base_url + path, json=json, headers=headers) as r:
            r.raise_for_status()
            return await r.json()


def _today_kst() -> date:
    return datetime.now(KST).date()


async def _ws_connect(url: str):
    return await websockets.connect(url, ping_interval=30, ping_timeout=20)


async def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")

    app_key = os.environ["KIS_APP_KEY"]
    app_secret = os.environ["KIS_APP_SECRET"]
    base_url = os.environ.get("KIS_BASE_URL", "https://openapi.koreainvestment.com:9443")
    ws_url = os.environ.get("KIS_WS_URL", "ws://ops.koreainvestment.com:21000")
    symbols = [s.strip() for s in os.environ["KIS_SYMBOLS"].split(",") if s.strip()]
    bootstrap = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")
    topic = os.environ.get("KAFKA_TOPIC_TICK", "kis.tick.raw")

    health_metrics.start(port=int(os.environ.get("METRICS_PORT", "9100")))

    async with aiohttp.ClientSession() as session:
        auth = KisAuth(
            app_key=app_key, app_secret=app_secret, base_url=base_url,
            http=_AiohttpKisHttp(base_url, session),
        )
        # initial refresh on startup so WS subscribe has approval_key
        await auth.refresh()

        producer = AIOKafkaProducer(
            bootstrap_servers=bootstrap,
            acks="all",
            enable_idempotence=True,
            linger_ms=50,
            compression_type="snappy",
            max_in_flight_requests_per_connection=5,
        )
        await producer.start()
        try:
            publisher = KafkaTickPublisher(
                topic=topic,
                send=lambda t, v, k: producer.send_and_wait(t, value=v, key=k),
            )

            ws = KisWebSocket(
                ws_url=ws_url, auth=auth, symbols=symbols,
                ws_connect=_ws_connect,
                on_connect=lambda b: health_metrics.ws_connected.set(1 if b else 0),
                business_day_provider=_today_kst,
            )

            refresher = asyncio.create_task(
                token_refresher.run(auth, on_failure=lambda _: health_metrics.token_refresh_failures.inc())
            )

            async for row in ws.stream():
                try:
                    await publisher.publish(row)
                    health_metrics.messages_published.labels(symbol=row["symbol"]).inc()
                except Exception as e:  # noqa: BLE001
                    log.error("publish failed: %s", e)
        finally:
            refresher.cancel()
            await producer.stop()


if __name__ == "__main__":
    asyncio.run(main())
```

- [ ] **Step 3: Commit**

```bash
git add infra/docker/kis-producer/main.py infra/docker/kis-producer/requirements.txt
git commit -m "feat(producer): KIS producer entrypoint wiring auth, ws, kafka, refresher, metrics"
```

---

### Task 14: KIS Producer Dockerfile + compose service

**Files:**
- Create: `infra/docker/kis-producer/Dockerfile`
- Modify: `infra/docker/docker-compose.yml` (add `kis-producer` service)

- [ ] **Step 1: Dockerfile — `infra/docker/kis-producer/Dockerfile`**

```dockerfile
FROM python:3.11-slim
WORKDIR /app
ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY src ./src
COPY main.py .
EXPOSE 9100
CMD ["python", "-u", "main.py"]
```

- [ ] **Step 2: compose service 추가 — `infra/docker/docker-compose.yml` services 끝에 append**

```yaml
  kis-producer:
    build: ./kis-producer
    container_name: tickberg-kis-producer
    restart: unless-stopped
    env_file: ../../.env
    environment:
      KAFKA_BOOTSTRAP: kafka:9092
      METRICS_PORT: "9100"
      TZ: Asia/Seoul
    depends_on:
      kafka:
        condition: service_healthy
    ports: ["9100:9100"]
```

- [ ] **Step 3: Build + run + 검증**

```bash
docker compose -f infra/docker/docker-compose.yml build kis-producer
docker compose -f infra/docker/docker-compose.yml up -d kis-producer
docker logs tickberg-kis-producer --tail 30
```
Expected: `subscribed 3 symbols` 또는 (장 마감 후) `next refresh at 2026-05-08T03:30:00+09:00 ...`. 영업시간 중이면 metrics 확인:
```bash
curl -s localhost:9100/metrics | grep kis_messages_published_total
```

- [ ] **Step 4: Commit**

```bash
git add infra/docker/kis-producer/Dockerfile infra/docker/docker-compose.yml
git commit -m "feat(producer): KIS producer Dockerfile and compose service"
```

---

### Task 15: Spark image with Iceberg + AWS jars + Kafka connector

부트캠프 Dockerfile pattern (Iceberg 1.5.2 + hadoop-aws 3.3.4 + aws-sdk-bundle 1.12.262) 재사용. bitnami/spark:3.5.1 베이스 + Kafka connector (Streaming source) + Prometheus servlet.

**Files:**
- Create: `infra/docker/spark/Dockerfile`
- Create: `infra/docker/spark/spark-defaults.conf`
- Create: `infra/docker/spark/metrics.properties`

- [ ] **Step 1: Dockerfile — `infra/docker/spark/Dockerfile`**

```dockerfile
FROM bitnami/spark:3.5.1

USER root

ARG ICEBERG_VERSION=1.5.2
ARG SPARK_MAJOR=3.5
ARG SCALA_MAJOR=2.12
ARG HADOOP_AWS_VERSION=3.3.4
ARG AWS_SDK_BUNDLE_VERSION=1.12.262
ARG KAFKA_CLIENT_VERSION=3.5.1

WORKDIR /opt/bitnami/spark/jars

# Iceberg runtime + AWS bundle (S3 FileIO + Glue Catalog v2 SDK)
RUN curl -fsSLO "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-${SPARK_MAJOR}_${SCALA_MAJOR}/${ICEBERG_VERSION}/iceberg-spark-runtime-${SPARK_MAJOR}_${SCALA_MAJOR}-${ICEBERG_VERSION}.jar" \
 && curl -fsSLO "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/${ICEBERG_VERSION}/iceberg-aws-bundle-${ICEBERG_VERSION}.jar" \
 && curl -fsSLO "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_AWS_VERSION}/hadoop-aws-${HADOOP_AWS_VERSION}.jar" \
 && curl -fsSLO "https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_BUNDLE_VERSION}/aws-java-sdk-bundle-${AWS_SDK_BUNDLE_VERSION}.jar" \
 && curl -fsSLO "https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_${SCALA_MAJOR}/3.5.1/spark-sql-kafka-0-10_${SCALA_MAJOR}-3.5.1.jar" \
 && curl -fsSLO "https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_${SCALA_MAJOR}/3.5.1/spark-token-provider-kafka-0-10_${SCALA_MAJOR}-3.5.1.jar" \
 && curl -fsSLO "https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/${KAFKA_CLIENT_VERSION}/kafka-clients-${KAFKA_CLIENT_VERSION}.jar" \
 && curl -fsSLO "https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar"

COPY spark-defaults.conf /opt/bitnami/spark/conf/spark-defaults.conf
COPY metrics.properties /opt/bitnami/spark/conf/metrics.properties

USER 1001
```

- [ ] **Step 2: `infra/docker/spark/spark-defaults.conf`**

Iceberg/Glue/S3 default config + Prometheus servlet 활성화.

```properties
# AWS credentials via ~/.aws (mounted) + AWS_PROFILE env
spark.hadoop.fs.s3a.aws.credentials.provider=com.amazonaws.auth.profile.ProfileCredentialsProvider
spark.hadoop.com.amazonaws.profile.name=tickberg
spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem
spark.hadoop.fs.s3a.endpoint=s3.ap-northeast-2.amazonaws.com

# Iceberg SparkSessionCatalog + Glue
spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions
spark.sql.catalog.glue=org.apache.iceberg.spark.SparkCatalog
spark.sql.catalog.glue.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog
spark.sql.catalog.glue.warehouse=s3://tickberg-lakehouse/
spark.sql.catalog.glue.io-impl=org.apache.iceberg.aws.s3.S3FileIO

# Use glue catalog by default
spark.sql.defaultCatalog=glue

# Prometheus servlet (driver / executor / master / worker)
spark.metrics.conf=/opt/bitnami/spark/conf/metrics.properties

# Sensible streaming defaults
spark.sql.streaming.checkpointFailFast=true
spark.sql.adaptive.enabled=true
```

- [ ] **Step 3: `infra/docker/spark/metrics.properties`**

```properties
*.sink.prometheusServlet.class=org.apache.spark.metrics.sink.PrometheusServlet
*.sink.prometheusServlet.path=/metrics/prometheus
master.sink.prometheusServlet.path=/metrics/master/prometheus
applications.sink.prometheusServlet.path=/metrics/applications/prometheus
```

- [ ] **Step 4: Build**

```bash
docker compose -f infra/docker/docker-compose.yml build spark-master
docker compose -f infra/docker/docker-compose.yml up -d spark-master spark-worker
sleep 5
curl -s localhost:8081/json/ | head -20
```
Expected: Spark master JSON 응답 + worker registered.

- [ ] **Step 5: Iceberg/Glue 스모크 — Spark master 안에서 SQL**

```bash
docker exec tickberg-spark-master spark-sql -e "SHOW TABLES IN glue.tickberg;"
```
Expected: `bronze_kis_tick_raw`, `silver_kis_tick_clean`, `silver_dim_symbol`, `gold_symbol_vwap_1m` 출력.

- [ ] **Step 6: Commit**

```bash
git add infra/docker/spark/
git commit -m "feat(infra): Spark image with Iceberg 1.5.2 + AWS bundle + Kafka + Prometheus servlet"
```

---

### Task 16: Spark Streaming Bronze (Kafka → Parquet, 1min trigger)

Spec §4.1 — `processingTime=1min`, hour partition, S3 checkpoint.

**Files:**
- Create: `code/pipelines/__init__.py`
- Create: `code/pipelines/bronze/__init__.py`
- Create: `code/pipelines/bronze/bronze_kis_tick_streaming.py`
- Modify: `infra/docker/docker-compose.yml` (add `spark-streaming` service)

- [ ] **Step 1: streaming job — `code/pipelines/bronze/bronze_kis_tick_streaming.py`**

```python
"""Spark Structured Streaming: Kafka kis.tick.raw → S3 Bronze Parquet.

Trigger 1min, append, partition (dt, hr). Long-running outside Airflow.
"""
from __future__ import annotations

import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DecimalType, IntegerType, LongType, StringType, StructField, StructType, TimestampType,
)

BUCKET = os.environ.get("S3_BUCKET", "tickberg-lakehouse")
TOPIC = os.environ.get("KAFKA_TOPIC_TICK", "kis.tick.raw")
BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")

OUTPUT_PATH = f"s3a://{BUCKET}/bronze/kis_tick_raw/"
CHECKPOINT_PATH = f"s3a://{BUCKET}/checkpoints/bronze_kis_tick/"


_SCHEMA = StructType([
    StructField("symbol", StringType()),
    StructField("trade_ts_kst", StringType()),       # ISO string from producer
    StructField("price", StringType()),              # Decimal as string
    StructField("trade_side", StringType()),
    StructField("volume", LongType()),
    StructField("best_ask_price", StringType()),
    StructField("best_bid_price", StringType()),
    StructField("cum_volume", LongType()),
    StructField("cum_amount", LongType()),
    StructField("raw_payload", StringType()),
])


def build_query(spark: SparkSession):
    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", BOOTSTRAP)
        .option("subscribe", TOPIC)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )

    parsed = (
        raw.select(
            F.col("partition").alias("kafka_partition"),
            F.col("offset").alias("kafka_offset"),
            F.from_json(F.col("value").cast("string"), _SCHEMA).alias("j"),
            F.current_timestamp().alias("ingest_ts"),
        )
        .select(
            "ingest_ts", "kafka_partition", "kafka_offset",
            F.col("j.symbol").alias("symbol"),
            F.to_timestamp("j.trade_ts_kst").alias("trade_ts_kst"),
            F.col("j.price").cast(DecimalType(18, 2)).alias("price"),
            F.col("j.volume").alias("volume"),
            F.col("j.cum_volume").alias("cum_volume"),
            F.col("j.cum_amount").alias("cum_amount"),
            F.col("j.trade_side").alias("trade_side"),
            F.col("j.best_ask_price").cast(DecimalType(18, 2)).alias("best_ask_price"),
            F.col("j.best_bid_price").cast(DecimalType(18, 2)).alias("best_bid_price"),
            F.col("j.raw_payload").alias("raw_payload"),
        )
        .withColumn("dt", F.to_date("trade_ts_kst"))
        .withColumn("hr", F.hour("trade_ts_kst"))
    )

    return (
        parsed.writeStream
        .format("parquet")
        .option("path", OUTPUT_PATH)
        .option("checkpointLocation", CHECKPOINT_PATH)
        .partitionBy("dt", "hr")
        .outputMode("append")
        .trigger(processingTime="1 minute")
        .queryName("bronze_kis_tick_streaming")
        .start()
    )


def main() -> None:
    spark = (
        SparkSession.builder.appName("bronze_kis_tick_streaming")
        .config("spark.sql.session.timeZone", "Asia/Seoul")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    q = build_query(spark)
    q.awaitTermination()


if __name__ == "__main__":
    main()
```

- [ ] **Step 2: `__init__.py` 빈 파일**

```bash
touch code/pipelines/__init__.py code/pipelines/bronze/__init__.py
```

- [ ] **Step 3: compose 에 `spark-streaming` 서비스 추가 — `infra/docker/docker-compose.yml`**

```yaml
  spark-streaming:
    image: tickberg/spark:3.5.1-iceberg-1.5.2
    container_name: tickberg-spark-streaming
    restart: unless-stopped
    depends_on:
      - spark-master
      - spark-worker
      - kafka
    environment:
      AWS_PROFILE: ${AWS_PROFILE:-tickberg}
      AWS_REGION: ${AWS_REGION:-ap-northeast-2}
      S3_BUCKET: ${S3_BUCKET:-tickberg-lakehouse}
      KAFKA_BOOTSTRAP: kafka:9092
      KAFKA_TOPIC_TICK: ${KAFKA_TOPIC_TICK:-kis.tick.raw}
    volumes:
      - ../../code:/opt/spark/code
      - ~/.aws:/opt/bitnami/spark/.aws:ro
    command: >
      spark-submit
        --master spark://spark-master:7077
        --deploy-mode client
        --conf spark.driver.host=tickberg-spark-streaming
        --conf spark.driver.port=4041
        --conf spark.driver.bindAddress=0.0.0.0
        --conf spark.ui.port=4040
        --conf spark.scheduler.mode=FAIR
        --conf spark.scheduler.allocation.file=/opt/bitnami/spark/conf/fairscheduler.xml
        --executor-memory 2g
        --total-executor-cores 2
        /opt/spark/code/pipelines/bronze/bronze_kis_tick_streaming.py
    ports: ["4040:4040"]
```

- [ ] **Step 4: FairScheduler pool 설정 — `infra/docker/spark/fairscheduler.xml`**

Spec §2.3 (b) — streaming pool 우선↑, batch pool 양보 가능. 

```xml
<?xml version="1.0"?>
<allocations>
  <pool name="streaming_pool">
    <schedulingMode>FAIR</schedulingMode>
    <weight>3</weight>
    <minShare>2</minShare>
  </pool>
  <pool name="batch_pool">
    <schedulingMode>FAIR</schedulingMode>
    <weight>1</weight>
    <minShare>0</minShare>
  </pool>
</allocations>
```

이 파일을 spark image COPY 에 추가 — `infra/docker/spark/Dockerfile` 의 `COPY metrics.properties` 줄 뒤에 추가:

```dockerfile
COPY fairscheduler.xml /opt/bitnami/spark/conf/fairscheduler.xml
```

그리고 image 재빌드:
```bash
docker compose -f infra/docker/docker-compose.yml build spark-master
```

- [ ] **Step 5: 기동 + 검증 (장 시간대 기준)**

```bash
docker compose -f infra/docker/docker-compose.yml up -d spark-streaming
sleep 60
docker logs tickberg-spark-streaming --tail 50
```
Expected: `bronze_kis_tick_streaming` query started, micro-batch 로그.

영업시간이면 1–2분 후 S3 확인:
```bash
aws --profile tickberg s3 ls s3://tickberg-lakehouse/bronze/kis_tick_raw/ --recursive | head
```
Expected: `dt=2026-05-XX/hr=HH/part-*.parquet` 파일.

영업시간 외 (idle) 면 checkpoint 디렉토리만 생성됨:
```bash
aws --profile tickberg s3 ls s3://tickberg-lakehouse/checkpoints/bronze_kis_tick/
```
Expected: `commits/`, `offsets/`, `sources/`.

- [ ] **Step 6: Commit**

```bash
git add code/pipelines/ infra/docker/docker-compose.yml infra/docker/spark/Dockerfile infra/docker/spark/fairscheduler.xml
git commit -m "feat(streaming): Spark Structured Streaming Bronze (Kafka → Parquet 1min trigger)"
```

---

### Task 17: Bronze→Silver MERGE INTO (TDD — dedup integration test = demo prep)

Spec §3.5 — Iceberg ① MERGE 시연 위치. Spec §7.3 — 통합 테스트 = demo prep 겸용.  
local Spark + local Iceberg Hadoop catalog 로 dedup 시나리오 검증 (AWS 미사용).

**Files:**
- Create: `code/pipelines/silver/__init__.py`
- Create: `code/pipelines/silver/bronze_to_silver_kis_tick.py`
- Create: `tests/test_silver_merge.py`
- Create: `tests/spark_fixtures.py`

- [ ] **Step 1: Spark fixture — `tests/spark_fixtures.py`**

```python
"""Local Spark session fixture — Hadoop catalog (no AWS)."""
from __future__ import annotations

import shutil
import tempfile
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

ICEBERG_PKG = "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2"


@pytest.fixture(scope="session")
def warehouse_dir():
    d = tempfile.mkdtemp(prefix="tickberg-iceberg-")
    yield Path(d)
    shutil.rmtree(d, ignore_errors=True)


@pytest.fixture(scope="session")
def spark(warehouse_dir):
    s = (
        SparkSession.builder
        .appName("tickberg-tests")
        .master("local[2]")
        .config("spark.jars.packages", ICEBERG_PKG)
        .config("spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.local.type", "hadoop")
        .config("spark.sql.catalog.local.warehouse", str(warehouse_dir))
        .config("spark.sql.session.timeZone", "Asia/Seoul")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    s.sparkContext.setLogLevel("WARN")
    yield s
    s.stop()
```

- [ ] **Step 2: 실패 테스트 — `tests/test_silver_merge.py`**

```python
"""Demo prep test: 동일 trade_uid 가 두 번 들어오면 MERGE 후 1건만 적재됨."""
from __future__ import annotations

from datetime import datetime
from decimal import Decimal

import pytest
from pyspark.sql import Row

from pipelines.silver.bronze_to_silver_kis_tick import merge_bronze_into_silver
from tests.spark_fixtures import spark, warehouse_dir   # noqa: F401


def _bronze_row(*, symbol="005930", trade_dt=datetime(2026, 5, 8, 9, 30, 1),
                price="72500", volume=100, cum_volume=1234567):
    return Row(
        ingest_ts=datetime(2026, 5, 8, 0, 30, 5),
        kafka_partition=0, kafka_offset=42,
        symbol=symbol, trade_ts_kst=trade_dt,
        price=Decimal(price), volume=volume,
        cum_volume=cum_volume, cum_amount=cum_volume * int(price),
        trade_side="+", best_ask_price=Decimal(price),
        best_bid_price=Decimal(str(int(price) - 100)),
        raw_payload="dummy",
    )


@pytest.fixture
def silver_table(spark):
    spark.sql("CREATE NAMESPACE IF NOT EXISTS local.silver")
    spark.sql("DROP TABLE IF EXISTS local.silver.kis_tick_clean")
    spark.sql("""
      CREATE TABLE local.silver.kis_tick_clean (
        trade_uid string, symbol string,
        trade_ts_kst timestamp, trade_ts_utc timestamp,
        price decimal(18,2), volume bigint, trade_amount decimal(20,2),
        trade_side string, best_ask_price decimal(18,2), best_bid_price decimal(18,2),
        ingest_ts timestamp, silver_ts timestamp
      ) USING iceberg
      PARTITIONED BY (days(trade_ts_kst), hours(trade_ts_kst))
      TBLPROPERTIES ('format-version'='2')
    """)
    yield "local.silver.kis_tick_clean"


def test_duplicate_trade_uid_merged_to_one_row(spark, silver_table):
    bronze = spark.createDataFrame([_bronze_row(), _bronze_row()])  # 같은 trade_uid 2회

    merge_bronze_into_silver(spark, bronze_df=bronze, silver_table=silver_table)

    result = spark.sql(f"SELECT count(*) AS c FROM {silver_table}").collect()[0]
    assert result.c == 1


def test_merge_preserves_distinct_rows(spark, silver_table):
    rows = [
        _bronze_row(cum_volume=1),
        _bronze_row(cum_volume=2),
        _bronze_row(symbol="000660", cum_volume=1),
    ]
    bronze = spark.createDataFrame(rows)

    merge_bronze_into_silver(spark, bronze_df=bronze, silver_table=silver_table)

    out = spark.sql(f"SELECT symbol, count(*) c FROM {silver_table} GROUP BY symbol ORDER BY symbol")
    assert [(r.symbol, r.c) for r in out.collect()] == [("000660", 1), ("005930", 2)]


def test_second_run_with_overlapping_window_idempotent(spark, silver_table):
    bronze = spark.createDataFrame([_bronze_row(cum_volume=10)])
    merge_bronze_into_silver(spark, bronze_df=bronze, silver_table=silver_table)
    merge_bronze_into_silver(spark, bronze_df=bronze, silver_table=silver_table)

    c = spark.sql(f"SELECT count(*) c FROM {silver_table}").collect()[0].c
    assert c == 1
```

- [ ] **Step 3: Run failing**

```bash
pip install pyspark==3.5.1
pytest tests/test_silver_merge.py -v
```
Expected: ImportError on `pipelines.silver.bronze_to_silver_kis_tick`.

- [ ] **Step 4: Implement — `code/pipelines/silver/bronze_to_silver_kis_tick.py`**

```python
"""Bronze → Silver MERGE INTO with synthetic trade_uid dedup.

Airflow 호출: spark-submit ... --catalog glue, --window-minutes 10
Test 호출: merge_bronze_into_silver(spark, bronze_df=..., silver_table='local....')
"""
from __future__ import annotations

import argparse
import os

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


def _enrich(bronze_df: DataFrame) -> DataFrame:
    """Add trade_uid + trade_ts_utc + trade_amount + silver_ts."""
    return (
        bronze_df
        .withColumn(
            "trade_uid",
            F.concat_ws("_", F.col("symbol"),
                        F.date_format("trade_ts_kst", "yyyyMMddHHmmss"),
                        F.col("cum_volume").cast("string")),
        )
        .withColumn("trade_ts_utc",
                    F.from_utc_timestamp(F.to_utc_timestamp("trade_ts_kst", "Asia/Seoul"), "UTC"))
        .withColumn("trade_amount", (F.col("price") * F.col("volume")).cast("decimal(20,2)"))
        .withColumn("silver_ts", F.current_timestamp())
        .select(
            "trade_uid", "symbol", "trade_ts_kst", "trade_ts_utc",
            "price", "volume", "trade_amount", "trade_side",
            "best_ask_price", "best_bid_price", "ingest_ts", "silver_ts",
        )
    )


def merge_bronze_into_silver(spark: SparkSession, *, bronze_df: DataFrame, silver_table: str) -> int:
    """Idempotent MERGE on trade_uid. Return inserted-or-updated count (best effort)."""
    enriched = _enrich(bronze_df)
    enriched.createOrReplaceTempView("_bronze_stage")

    spark.sql(f"""
      MERGE INTO {silver_table} t
      USING (SELECT * FROM _bronze_stage) s
      ON  t.trade_uid = s.trade_uid
      WHEN NOT MATCHED THEN INSERT *
    """)
    return enriched.count()


def _read_bronze_window(spark: SparkSession, bucket: str, window_minutes: int) -> DataFrame:
    """Read recent N minutes of Bronze. Caller passes Glue-catalog table."""
    return (
        spark.table("glue.tickberg.bronze_kis_tick_raw")
        .where(F.col("ingest_ts") >= F.current_timestamp() - F.expr(f"INTERVAL {window_minutes} MINUTES"))
    )


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--window-minutes", type=int, default=10)
    p.add_argument("--silver-table", default="glue.tickberg.silver_kis_tick_clean")
    args = p.parse_args()

    bucket = os.environ.get("S3_BUCKET", "tickberg-lakehouse")
    spark = SparkSession.builder.appName("bronze_to_silver_kis_tick").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    spark.sparkContext.setLocalProperty("spark.scheduler.pool", "batch_pool")

    bronze = _read_bronze_window(spark, bucket=bucket, window_minutes=args.window_minutes)
    n = merge_bronze_into_silver(spark, bronze_df=bronze, silver_table=args.silver_table)
    print(f"merged window={args.window_minutes}min rows={n} table={args.silver_table}")


if __name__ == "__main__":
    main()
```

- [ ] **Step 5: `code/pipelines/silver/__init__.py` 빈 파일**

```bash
touch code/pipelines/silver/__init__.py
```

- [ ] **Step 6: pyproject.toml 의 pythonpath 보강**

`pyproject.toml` 의 `[tool.pytest.ini_options]` 의 pythonpath 에 `"."` 추가하여 `pipelines.silver.*` import 가능하게:

```toml
pythonpath = [".", "infra/docker/kis-producer", "code"]
```

- [ ] **Step 7: Run + verify pass**

```bash
pytest tests/test_silver_merge.py -v
```
Expected: 3 passed.

- [ ] **Step 8: Commit**

```bash
git add code/pipelines/silver/ tests/test_silver_merge.py tests/spark_fixtures.py pyproject.toml
git commit -m "feat(silver): Bronze→Silver MERGE with dedup (TDD, demo prep test)"
```

---

### Task 18: Silver→Gold OVERWRITE (hour partition)

Spec §3.4 — 5분마다 현재 hour 파티션 OVERWRITE.

**Files:**
- Create: `code/pipelines/gold/__init__.py`
- Create: `code/pipelines/gold/silver_to_gold_vwap.py`
- Create: `tests/test_gold_vwap.py`

- [ ] **Step 1: 실패 테스트 — `tests/test_gold_vwap.py`**

```python
from __future__ import annotations

from datetime import datetime
from decimal import Decimal

import pytest
from pyspark.sql import Row

from pipelines.gold.silver_to_gold_vwap import compute_vwap_for_hour
from tests.spark_fixtures import spark, warehouse_dir   # noqa: F401


def _silver_row(symbol, trade_ts, price, volume):
    return Row(
        trade_uid=f"{symbol}_{trade_ts.strftime('%Y%m%d%H%M%S')}_{volume}",
        symbol=symbol, trade_ts_kst=trade_ts,
        trade_ts_utc=trade_ts,
        price=Decimal(price), volume=volume,
        trade_amount=Decimal(price) * volume,
        trade_side="+", best_ask_price=Decimal(price),
        best_bid_price=Decimal(price), ingest_ts=trade_ts, silver_ts=trade_ts,
    )


def test_vwap_aggregates_by_minute(spark):
    rows = [
        _silver_row("005930", datetime(2026, 5, 8, 9, 30, 5), "72500", 100),
        _silver_row("005930", datetime(2026, 5, 8, 9, 30, 30), "72600", 200),
        _silver_row("005930", datetime(2026, 5, 8, 9, 31, 0), "72700", 50),
    ]
    silver = spark.createDataFrame(rows)

    out = compute_vwap_for_hour(spark, silver_df=silver, hour_kst=datetime(2026, 5, 8, 9, 0, 0))
    rows_out = {r.ts_minute.minute: r for r in out.collect()}

    assert rows_out[30].total_volume == 300
    assert rows_out[30].open_price == Decimal("72500.00")
    assert rows_out[30].close_price == Decimal("72600.00")
    assert rows_out[30].high_price == Decimal("72600.00")
    assert rows_out[30].low_price == Decimal("72500.00")
    expected_vwap = (Decimal("72500") * 100 + Decimal("72600") * 200) / 300
    assert abs(rows_out[30].vwap - expected_vwap) < Decimal("0.01")
    assert rows_out[30].trade_count == 2
    assert rows_out[31].total_volume == 50
```

- [ ] **Step 2: Run failing**

```bash
pytest tests/test_gold_vwap.py -v
```

- [ ] **Step 3: Implement — `code/pipelines/gold/silver_to_gold_vwap.py`**

```python
"""Silver → Gold: 1-minute VWAP, hour partition OVERWRITE.

Airflow trigger: 5분마다. 현재 hour 파티션만 dynamic overwrite.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

KST = ZoneInfo("Asia/Seoul")


def compute_vwap_for_hour(spark: SparkSession, *, silver_df: DataFrame, hour_kst: datetime) -> DataFrame:
    hour_start = hour_kst.replace(minute=0, second=0, microsecond=0)
    hour_end = hour_start + timedelta(hours=1)

    df = (
        silver_df
        .where((F.col("trade_ts_kst") >= F.lit(hour_start))
               & (F.col("trade_ts_kst") < F.lit(hour_end)))
        .withColumn("ts_minute", F.date_trunc("minute", "trade_ts_kst"))
    )

    w_open = Window.partitionBy("symbol", "ts_minute").orderBy("trade_ts_kst")
    w_close = Window.partitionBy("symbol", "ts_minute").orderBy(F.col("trade_ts_kst").desc())

    enriched = (
        df
        .withColumn("open_price", F.first("price").over(w_open))
        .withColumn("close_price", F.first("price").over(w_close))
    )

    return (
        enriched.groupBy("symbol", "ts_minute")
        .agg(
            F.first("open_price").alias("open_price"),
            F.first("close_price").alias("close_price"),
            F.max("price").alias("high_price"),
            F.min("price").alias("low_price"),
            F.sum("volume").alias("total_volume"),
            (F.sum(F.col("price") * F.col("volume")) / F.sum("volume")).cast("decimal(18,4)").alias("vwap"),
            F.count("*").cast("int").alias("trade_count"),
            F.current_timestamp().alias("computed_at"),
        )
        .orderBy("symbol", "ts_minute")
    )


def _current_hour_kst() -> datetime:
    return datetime.now(KST).replace(minute=0, second=0, microsecond=0, tzinfo=None)


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--hour", default=None, help="ISO hour KST (yyyy-MM-ddTHH); default = current")
    p.add_argument("--silver-table", default="glue.tickberg.silver_kis_tick_clean")
    p.add_argument("--gold-table", default="glue.tickberg.gold_symbol_vwap_1m")
    args = p.parse_args()

    hour = (
        datetime.fromisoformat(args.hour) if args.hour else _current_hour_kst()
    ).replace(minute=0, second=0, microsecond=0)

    spark = SparkSession.builder.appName("silver_to_gold_vwap").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    spark.sparkContext.setLocalProperty("spark.scheduler.pool", "batch_pool")
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

    silver = (
        spark.table(args.silver_table)
        .where((F.col("trade_ts_kst") >= F.lit(hour))
               & (F.col("trade_ts_kst") < F.lit(hour + timedelta(hours=1))))
    )
    out = compute_vwap_for_hour(spark, silver_df=silver, hour_kst=hour)
    out.writeTo(args.gold_table).overwritePartitions()
    print(f"gold overwrite hour={hour.isoformat()} rows={out.count()}")


if __name__ == "__main__":
    main()
```

- [ ] **Step 4: `code/pipelines/gold/__init__.py` 빈 파일 + Run + Commit**

```bash
touch code/pipelines/gold/__init__.py
pytest tests/test_gold_vwap.py -v
git add code/pipelines/gold/ tests/test_gold_vwap.py
git commit -m "feat(gold): Silver→Gold 1m VWAP with hour partition OVERWRITE"
```

---

### Task 19: dim_symbol daily MERGE (KIS REST → silver.dim_symbol)

Spec §3.3 — SCD1 + Iceberg time-travel. 04:00 KST 일배치, KIS REST `inquire-price` 또는 `search-stock-info` 사용.

**Files:**
- Create: `code/pipelines/silver/dim_symbol_daily.py`
- Create: `tests/test_dim_symbol_merge.py`

- [ ] **Step 1: 실패 테스트 — `tests/test_dim_symbol_merge.py`**

```python
from __future__ import annotations

from decimal import Decimal

import pytest
from pyspark.sql import Row

from pipelines.silver.dim_symbol_daily import merge_dim_symbol
from tests.spark_fixtures import spark, warehouse_dir   # noqa: F401


@pytest.fixture
def dim_table(spark):
    spark.sql("CREATE NAMESPACE IF NOT EXISTS local.silver")
    spark.sql("DROP TABLE IF EXISTS local.silver.dim_symbol")
    spark.sql("""
      CREATE TABLE local.silver.dim_symbol (
        symbol string, symbol_name string, market string,
        par_value decimal(18,2), shares_outstanding bigint,
        is_active boolean, updated_ts timestamp
      ) USING iceberg PARTITIONED BY (market)
      TBLPROPERTIES ('format-version'='2')
    """)
    yield "local.silver.dim_symbol"


def _row(symbol, name, market, par, shares, active=True):
    from datetime import datetime
    return Row(symbol=symbol, symbol_name=name, market=market,
               par_value=Decimal(par), shares_outstanding=shares,
               is_active=active, updated_ts=datetime(2026, 5, 8, 4, 0, 0))


def test_initial_insert(spark, dim_table):
    src = spark.createDataFrame([
        _row("005930", "삼성전자", "KOSPI", "100", 5969782550),
        _row("000660", "SK하이닉스", "KOSPI", "5000", 728002365),
    ])
    merge_dim_symbol(spark, src_df=src, dim_table=dim_table)
    c = spark.sql(f"SELECT count(*) c FROM {dim_table}").collect()[0].c
    assert c == 2


def test_update_existing_symbol(spark, dim_table):
    src1 = spark.createDataFrame([_row("005930", "삼성전자", "KOSPI", "100", 5969782550)])
    merge_dim_symbol(spark, src_df=src1, dim_table=dim_table)
    src2 = spark.createDataFrame([_row("005930", "삼성전자(액면분할)", "KOSPI", "100", 11939565100)])
    merge_dim_symbol(spark, src_df=src2, dim_table=dim_table)
    rows = spark.sql(f"SELECT * FROM {dim_table}").collect()
    assert len(rows) == 1
    assert rows[0].shares_outstanding == 11939565100
    assert rows[0].symbol_name == "삼성전자(액면분할)"
```

- [ ] **Step 2: Implement — `code/pipelines/silver/dim_symbol_daily.py`**

```python
"""dim_symbol daily MERGE (SCD1).

KIS REST 호출은 KisRestClient 추상으로 분리 (테스트 가능). DAG는 main() 호출.
KIS API endpoint: /uapi/domestic-stock/v1/quotations/search-stock-info (TR_ID: CTPF1604R)
"""
from __future__ import annotations

import argparse
import os
from datetime import datetime
from decimal import Decimal

from pyspark.sql import DataFrame, Row, SparkSession


def merge_dim_symbol(spark: SparkSession, *, src_df: DataFrame, dim_table: str) -> None:
    src_df.createOrReplaceTempView("_dim_src")
    spark.sql(f"""
      MERGE INTO {dim_table} t
      USING (SELECT * FROM _dim_src) s
      ON t.symbol = s.symbol
      WHEN MATCHED THEN UPDATE SET
        symbol_name = s.symbol_name,
        market = s.market,
        par_value = s.par_value,
        shares_outstanding = s.shares_outstanding,
        is_active = s.is_active,
        updated_ts = s.updated_ts
      WHEN NOT MATCHED THEN INSERT *
    """)


def fetch_rows_from_kis(symbols: list[str], *, kis_client) -> list[Row]:
    """KIS REST 로 종목 메타 조회. kis_client 는 .get_stock_info(symbol) -> dict."""
    now = datetime.now()
    out: list[Row] = []
    for sym in symbols:
        info = kis_client.get_stock_info(sym)
        out.append(Row(
            symbol=sym,
            symbol_name=info.get("hts_kor_isnm", ""),
            market=info.get("rprs_mrkt_kor_name", "KOSPI"),
            par_value=Decimal(info.get("stck_fcam", "0")),
            shares_outstanding=int(info.get("lstg_stqt", 0)),
            is_active=info.get("delisting_yn", "N") == "N",
            updated_ts=now,
        ))
    return out


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--symbols", default=os.environ.get("KIS_SYMBOLS", "005930,000660,035420"))
    p.add_argument("--dim-table", default="glue.tickberg.silver_dim_symbol")
    args = p.parse_args()
    symbols = [s.strip() for s in args.symbols.split(",") if s.strip()]

    # Late import to keep test path clean
    from infra.docker.kis_producer.src.kis_rest import KisRestClient  # type: ignore

    client = KisRestClient.from_env()
    rows = fetch_rows_from_kis(symbols, kis_client=client)

    spark = SparkSession.builder.appName("dim_symbol_daily").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    spark.sparkContext.setLocalProperty("spark.scheduler.pool", "batch_pool")
    df = spark.createDataFrame(rows)
    merge_dim_symbol(spark, src_df=df, dim_table=args.dim_table)
    print(f"dim_symbol merged rows={len(rows)}")


if __name__ == "__main__":
    main()
```

- [ ] **Step 3: KIS REST client 추가 — `infra/docker/kis-producer/src/kis_rest.py`**

dim_symbol DAG가 KIS REST 도 호출. 03:30 token refresh 후 04:00에 사용.

```python
"""KIS REST client — search-stock-info wrapper.

DAG 가 매일 04:00 호출. 03:30 의 토큰을 디스크에서 읽거나 env로 주입.
Phase 1 단순화: 매 호출마다 새 oauth token 발급. 다중 종목 = 종목당 1회 REST.
"""
from __future__ import annotations

import os

import requests


class KisRestClient:
    def __init__(self, *, app_key: str, app_secret: str, base_url: str):
        self._app_key = app_key
        self._app_secret = app_secret
        self._base_url = base_url.rstrip("/")
        self._token: str | None = None

    @classmethod
    def from_env(cls) -> "KisRestClient":
        return cls(
            app_key=os.environ["KIS_APP_KEY"],
            app_secret=os.environ["KIS_APP_SECRET"],
            base_url=os.environ.get("KIS_BASE_URL", "https://openapi.koreainvestment.com:9443"),
        )

    def _ensure_token(self) -> None:
        if self._token:
            return
        r = requests.post(
            f"{self._base_url}/oauth2/tokenP", timeout=10,
            json={"grant_type": "client_credentials",
                  "appkey": self._app_key, "appsecret": self._app_secret},
        )
        r.raise_for_status()
        self._token = r.json()["access_token"]

    def get_stock_info(self, symbol: str) -> dict:
        self._ensure_token()
        r = requests.get(
            f"{self._base_url}/uapi/domestic-stock/v1/quotations/search-stock-info",
            timeout=10,
            params={"PDNO": symbol, "PRDT_TYPE_CD": "300"},
            headers={
                "authorization": f"Bearer {self._token}",
                "appkey": self._app_key,
                "appsecret": self._app_secret,
                "tr_id": "CTPF1604R",
                "custtype": "P",
            },
        )
        r.raise_for_status()
        body = r.json()
        if body.get("rt_cd") != "0":
            raise RuntimeError(f"KIS error symbol={symbol}: {body.get('msg1')}")
        return body.get("output", {})
```

- [ ] **Step 4: Run + commit**

```bash
pytest tests/test_dim_symbol_merge.py -v
git add code/pipelines/silver/dim_symbol_daily.py infra/docker/kis-producer/src/kis_rest.py tests/test_dim_symbol_merge.py
git commit -m "feat(silver): dim_symbol daily SCD1 MERGE + KIS REST client"
```

---

### Task 20: Iceberg Compaction job (rewrite_data_files)

Spec §4.2 — 18:00 KST MON-FRI. Silver/Gold 두 테이블 대상. target 384MB, 256–512MB 범위.

**Files:**
- Create: `code/pipelines/silver/iceberg_compaction.py`
- Create: `tests/test_iceberg_compaction.py`

- [ ] **Step 1: 실패 테스트 — `tests/test_iceberg_compaction.py`**

```python
"""Compaction의 file 수 감소 동작 검증 (small Spark + Hadoop catalog).

Iceberg Spark Action `rewrite_data_files` 는 SQL stored procedure 로도 호출 가능.
"""
from __future__ import annotations

from datetime import datetime
from decimal import Decimal

import pytest
from pyspark.sql import Row

from pipelines.silver.iceberg_compaction import compact_table
from tests.spark_fixtures import spark, warehouse_dir   # noqa: F401


@pytest.fixture
def small_files_table(spark):
    spark.sql("CREATE NAMESPACE IF NOT EXISTS local.silver")
    spark.sql("DROP TABLE IF EXISTS local.silver.small_files")
    spark.sql("""
      CREATE TABLE local.silver.small_files (
        id bigint, payload string, ts timestamp
      ) USING iceberg PARTITIONED BY (days(ts))
      TBLPROPERTIES ('format-version'='2', 'write.target-file-size-bytes'='268435456')
    """)
    base_ts = datetime(2026, 5, 8, 9, 0, 0)
    for i in range(10):
        rows = [Row(id=i*100+j, payload="x"*100, ts=base_ts) for j in range(100)]
        spark.createDataFrame(rows).writeTo("local.silver.small_files").append()
    yield "local.silver.small_files"


def test_compaction_reduces_file_count(spark, small_files_table):
    files_before = spark.sql(f"SELECT count(*) c FROM {small_files_table}.files").collect()[0].c
    assert files_before > 1
    compact_table(spark, table=small_files_table, target_file_size_bytes=384*1024*1024)
    files_after = spark.sql(f"SELECT count(*) c FROM {small_files_table}.files").collect()[0].c
    assert files_after < files_before
```

- [ ] **Step 2: Implement — `code/pipelines/silver/iceberg_compaction.py`**

```python
"""Iceberg compaction — rewrite_data_files SQL stored procedure.

Spec §4.2 — 18:00 KST MON-FRI. Silver/Gold target 384MB.
"""
from __future__ import annotations

import argparse

from pyspark.sql import SparkSession


def compact_table(spark: SparkSession, *, table: str, target_file_size_bytes: int = 384 * 1024 * 1024,
                  min_file_size_bytes: int = 256 * 1024 * 1024,
                  max_file_size_bytes: int = 512 * 1024 * 1024) -> dict:
    """Run Iceberg `rewrite_data_files` stored procedure on `table`.

    Returns map of metrics from the procedure call (rewritten_data_files_count etc).
    """
    catalog = table.split(".")[0]
    qualified = ".".join(table.split(".")[1:])
    sql = f"""
      CALL {catalog}.system.rewrite_data_files(
        table => '{qualified}',
        options => map(
          'target-file-size-bytes', '{target_file_size_bytes}',
          'min-file-size-bytes', '{min_file_size_bytes}',
          'max-file-size-bytes', '{max_file_size_bytes}',
          'rewrite-all', 'false'
        )
      )
    """
    row = spark.sql(sql).collect()
    return row[0].asDict() if row else {}


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--tables", nargs="+",
                   default=[
                       "glue.tickberg.silver_kis_tick_clean",
                       "glue.tickberg.gold_symbol_vwap_1m",
                   ])
    p.add_argument("--target-mb", type=int, default=384)
    args = p.parse_args()

    spark = SparkSession.builder.appName("iceberg_compaction").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    spark.sparkContext.setLocalProperty("spark.scheduler.pool", "batch_pool")
    target = args.target_mb * 1024 * 1024

    for t in args.tables:
        result = compact_table(spark, table=t, target_file_size_bytes=target)
        print(f"compacted {t}: {result}")


if __name__ == "__main__":
    main()
```

- [ ] **Step 3: Run + commit**

```bash
pytest tests/test_iceberg_compaction.py -v
git add code/pipelines/silver/iceberg_compaction.py tests/test_iceberg_compaction.py
git commit -m "feat(silver): Iceberg compaction job using rewrite_data_files procedure"
```

---

### Task 21: Airflow base — DAG 공통 helper + spark-submit BashOperator pattern

Airflow → Spark master 잡 트리거. Airflow container 안에서 `docker exec tickberg-spark-master spark-submit ...` 호출.  
docker.sock 마운트 + Airflow image에 docker-cli 추가 필요.

**Files:**
- Create: `infra/docker/airflow/Dockerfile`
- Modify: `infra/docker/docker-compose.yml` (Airflow image 변경, docker.sock 마운트)
- Create: `orchestration/dags/_common.py`

- [ ] **Step 1: `infra/docker/airflow/Dockerfile`**

```dockerfile
FROM apache/airflow:2.9.3-python3.11

USER root
RUN apt-get update \
 && apt-get install -y --no-install-recommends docker.io \
 && rm -rf /var/lib/apt/lists/*
USER airflow
RUN pip install --no-cache-dir apache-airflow-providers-amazon==8.24.0
```

- [ ] **Step 2: compose 수정 — `infra/docker/docker-compose.yml` 의 `x-airflow-common`**

```yaml
x-airflow-common: &airflow-common
  build: ./airflow
  image: tickberg/airflow:2.9.3
  environment: &airflow-env
    AIRFLOW__CORE__EXECUTOR: LocalExecutor
    AIRFLOW__CORE__DEFAULT_TIMEZONE: Asia/Seoul
    AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: postgresql+psycopg2://airflow:airflow@airflow-postgres:5432/airflow
    AIRFLOW__CORE__LOAD_EXAMPLES: "false"
    AIRFLOW__WEBSERVER__EXPOSE_CONFIG: "true"
    AWS_PROFILE: ${AWS_PROFILE:-tickberg}
    AWS_REGION: ${AWS_REGION:-ap-northeast-2}
    S3_BUCKET: ${S3_BUCKET:-tickberg-lakehouse}
  volumes:
    - ../../orchestration/dags:/opt/airflow/dags
    - ../../code:/opt/airflow/code
    - ~/.aws:/home/airflow/.aws:ro
    - airflow_logs:/opt/airflow/logs
    - /var/run/docker.sock:/var/run/docker.sock
  group_add:
    - "0"     # docker socket의 group (mac/linux 호환을 위한 root group)
  depends_on:
    airflow-postgres:
      condition: service_healthy
```

- [ ] **Step 3: 공통 helper — `orchestration/dags/_common.py`**

```python
"""DAG-common helpers.

spark_submit_in_master(script_path, *args) returns a bash command string
that runs spark-submit inside the spark-master container.
"""
from __future__ import annotations

SPARK_SUBMIT_PREFIX = (
    "docker exec tickberg-spark-master spark-submit "
    "--master spark://spark-master:7077 "
    "--deploy-mode client "
    "--conf spark.driver.host=tickberg-spark-master "
    "--conf spark.driver.bindAddress=0.0.0.0 "
    "--conf spark.scheduler.mode=FAIR "
    "--conf spark.scheduler.allocation.file=/opt/bitnami/spark/conf/fairscheduler.xml "
    "--executor-memory 2g --total-executor-cores 2 "
)


def spark_submit_command(script: str, *args: str) -> str:
    """Build BashOperator command for triggering Spark batch in spark-master container.

    Example:
      spark_submit_command(
        '/opt/spark/code/pipelines/silver/bronze_to_silver_kis_tick.py',
        '--window-minutes', '10',
      )
    """
    quoted_args = " ".join(f"'{a}'" for a in args)
    return f"set -euo pipefail; {SPARK_SUBMIT_PREFIX}{script} {quoted_args}"
```

- [ ] **Step 4: image 빌드 + restart**

```bash
docker compose -f infra/docker/docker-compose.yml build airflow-init
docker compose -f infra/docker/docker-compose.yml up -d airflow-init airflow-scheduler airflow-webserver
docker logs tickberg-airflow-web --tail 20
```
Expected: webserver 기동, http://localhost:8080 접근 가능.

- [ ] **Step 5: docker.sock 권한 sanity (Airflow scheduler 컨테이너에서 docker 명령 가능한지)**

```bash
docker exec tickberg-airflow-scheduler docker ps | head -3
```
Expected: container list 출력 (permission denied 시 mac에서는 group_add: ["0"] 그대로 OK, linux는 docker socket gid 확인 필요).

- [ ] **Step 6: Commit**

```bash
git add infra/docker/airflow/Dockerfile infra/docker/docker-compose.yml orchestration/dags/_common.py
git commit -m "feat(orchestration): Airflow image with docker-cli + spark-submit helper"
```

---

### Task 22: DAG `bronze_to_silver_kis` (`*/5 9-16 * * MON-FRI`)

Spec §4.2.

**Files:**
- Create: `orchestration/dags/bronze_to_silver_kis.py`

- [ ] **Step 1: DAG — `orchestration/dags/bronze_to_silver_kis.py`**

```python
"""Bronze → Silver 5-min MERGE during market hours (KST)."""
from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

from _common import spark_submit_command

default_args = {
    "owner": "tickberg",
    "depends_on_past": False,
    "email_on_failure": True,
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
    "sla": timedelta(minutes=10),
}

with DAG(
    dag_id="bronze_to_silver_kis",
    default_args=default_args,
    description="Merge recent Bronze KIS ticks into Silver (dedup by trade_uid)",
    schedule="*/5 9-16 * * MON-FRI",
    start_date=datetime(2026, 5, 11, 9, 0),
    catchup=False,
    max_active_runs=1,
    tags=["silver", "kis", "market-hours"],
) as dag:
    merge = BashOperator(
        task_id="merge_bronze_to_silver",
        bash_command=spark_submit_command(
            "/opt/spark/code/pipelines/silver/bronze_to_silver_kis_tick.py",
            "--window-minutes", "10",
            "--silver-table", "glue.tickberg.silver_kis_tick_clean",
        ),
    )
```

- [ ] **Step 2: 검증 — Airflow UI 에서 DAG 인식**

```bash
docker exec tickberg-airflow-scheduler airflow dags list | grep bronze_to_silver_kis
```
Expected: DAG 출력 + import error 없음.

- [ ] **Step 3: Manual test run (영업시간 외라도 1회 trigger)**

```bash
docker exec tickberg-airflow-scheduler airflow dags trigger bronze_to_silver_kis
sleep 30
docker exec tickberg-airflow-scheduler airflow tasks states-for-dag-run bronze_to_silver_kis $(docker exec tickberg-airflow-scheduler airflow dags list-runs -d bronze_to_silver_kis -o plain | head -2 | tail -1 | awk '{print $2}')
```
Expected: `success` (장 마감 후라 빈 윈도우 → no-op MERGE).

- [ ] **Step 4: Commit**

```bash
git add orchestration/dags/bronze_to_silver_kis.py
git commit -m "feat(orchestration): bronze_to_silver_kis DAG (*/5 market hours)"
```

---

### Task 23: DAG `silver_to_gold_vwap` (chained via ExternalTaskSensor)

Spec §4.2 — Bronze→Silver 끝나면 자동 트리거.

**Files:**
- Create: `orchestration/dags/silver_to_gold_vwap.py`

- [ ] **Step 1: DAG**

```python
"""Silver → Gold 1m VWAP, hour partition OVERWRITE.

Chained to bronze_to_silver_kis via ExternalTaskSensor (same execution_date).
"""
from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.sensors.external_task import ExternalTaskSensor

from _common import spark_submit_command

default_args = {
    "owner": "tickberg",
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
    "sla": timedelta(minutes=10),
    "email_on_failure": True,
}

with DAG(
    dag_id="silver_to_gold_vwap",
    default_args=default_args,
    schedule="*/5 9-16 * * MON-FRI",
    start_date=datetime(2026, 5, 11, 9, 0),
    catchup=False,
    max_active_runs=1,
    tags=["gold", "kis", "market-hours"],
) as dag:
    wait_silver = ExternalTaskSensor(
        task_id="wait_for_bronze_to_silver",
        external_dag_id="bronze_to_silver_kis",
        external_task_id="merge_bronze_to_silver",
        allowed_states=["success"],
        failed_states=["failed", "skipped"],
        mode="reschedule",
        poke_interval=30,
        timeout=60 * 5,
    )

    overwrite_gold = BashOperator(
        task_id="overwrite_gold_vwap",
        bash_command=spark_submit_command(
            "/opt/spark/code/pipelines/gold/silver_to_gold_vwap.py",
            "--silver-table", "glue.tickberg.silver_kis_tick_clean",
            "--gold-table", "glue.tickberg.gold_symbol_vwap_1m",
        ),
    )

    wait_silver >> overwrite_gold
```

- [ ] **Step 2: 검증 + Commit**

```bash
docker exec tickberg-airflow-scheduler airflow dags list | grep silver_to_gold_vwap
git add orchestration/dags/silver_to_gold_vwap.py
git commit -m "feat(orchestration): silver_to_gold_vwap DAG chained via ExternalTaskSensor"
```

---

### Task 24: DAG `dim_symbol_daily` + `iceberg_compaction`

Spec §4.2.

**Files:**
- Create: `orchestration/dags/dim_symbol_daily.py`
- Create: `orchestration/dags/iceberg_compaction.py`

- [ ] **Step 1: `orchestration/dags/dim_symbol_daily.py`**

```python
"""dim_symbol daily MERGE (04:00 KST). Uses KIS REST → SCD1 MERGE."""
from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

from _common import spark_submit_command

with DAG(
    dag_id="dim_symbol_daily",
    default_args={
        "owner": "tickberg",
        "retries": 3,
        "retry_delay": timedelta(minutes=2),
        "email_on_failure": True,
    },
    schedule="0 4 * * *",
    start_date=datetime(2026, 5, 11, 4, 0),
    catchup=False,
    max_active_runs=1,
    tags=["silver", "dim", "after-hours"],
) as dag:
    merge_dim = BashOperator(
        task_id="merge_dim_symbol",
        bash_command=spark_submit_command(
            "/opt/spark/code/pipelines/silver/dim_symbol_daily.py",
            "--symbols", "005930,000660,035420",
            "--dim-table", "glue.tickberg.silver_dim_symbol",
        ),
        env={
            "KIS_APP_KEY": "{{ var.value.KIS_APP_KEY }}",
            "KIS_APP_SECRET": "{{ var.value.KIS_APP_SECRET }}",
            "KIS_BASE_URL": "{{ var.value.get('KIS_BASE_URL', 'https://openapi.koreainvestment.com:9443') }}",
        },
    )
```

- [ ] **Step 2: `orchestration/dags/iceberg_compaction.py`**

```python
"""Iceberg compaction (18:00 KST MON-FRI) on Silver + Gold."""
from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

from _common import spark_submit_command

with DAG(
    dag_id="iceberg_compaction",
    default_args={
        "owner": "tickberg",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
        "email_on_failure": True,
    },
    schedule="0 18 * * MON-FRI",
    start_date=datetime(2026, 5, 11, 18, 0),
    catchup=False,
    max_active_runs=1,
    tags=["maintenance", "after-hours"],
) as dag:
    compact = BashOperator(
        task_id="rewrite_data_files",
        bash_command=spark_submit_command(
            "/opt/spark/code/pipelines/silver/iceberg_compaction.py",
            "--tables",
            "glue.tickberg.silver_kis_tick_clean",
            "glue.tickberg.gold_symbol_vwap_1m",
            "--target-mb", "384",
        ),
    )
```

- [ ] **Step 3: KIS Variables 등록 (Airflow CLI)**

```bash
docker exec tickberg-airflow-scheduler airflow variables set KIS_APP_KEY "$KIS_APP_KEY"
docker exec tickberg-airflow-scheduler airflow variables set KIS_APP_SECRET "$KIS_APP_SECRET"
```
Expected: `Variable KIS_APP_KEY created` (또는 updated).

- [ ] **Step 4: 검증 + Commit**

```bash
docker exec tickberg-airflow-scheduler airflow dags list | grep -E "dim_symbol_daily|iceberg_compaction"
git add orchestration/dags/dim_symbol_daily.py orchestration/dags/iceberg_compaction.py
git commit -m "feat(orchestration): dim_symbol_daily (04:00) + iceberg_compaction (18:00)"
```

---

### Task 25: Health queries 4개 (T4 ad-hoc 디버깅용)

Spec §5.5.

**Files:**
- Create: `code/health-queries/01_bronze_freshness.sql`
- Create: `code/health-queries/02_silver_dedup_rate.sql`
- Create: `code/health-queries/03_symbol_coverage.sql`
- Create: `code/health-queries/04_gold_partition_completeness.sql`
- Create: `infra/scripts/run_health_query.sh`

- [ ] **Step 1: `code/health-queries/01_bronze_freshness.sql`**

```sql
-- "데이터 N분 전 도착". 영업시간 정상 = 1–2분.
-- 영업시간 외 (장 마감 후) 는 lag 가 커지는게 정상이라 alert 임계값을 시간대 분기 (T1 Grafana)
SELECT
  CAST(date_diff('second', max(ingest_ts), current_timestamp) AS double) / 60 AS lag_minutes,
  max(ingest_ts) AS last_ingest,
  current_timestamp AS now_utc
FROM tickberg.bronze_kis_tick_raw
WHERE dt >= current_date - interval '1' day;
```

- [ ] **Step 2: `code/health-queries/02_silver_dedup_rate.sql`**

```sql
-- Bronze count vs Silver count 1h. 정상 0.95–1.0 (Silver=Bronze 또는 약간 적음).
WITH bronze_h AS (
  SELECT count(*) AS c FROM tickberg.bronze_kis_tick_raw
  WHERE ingest_ts >= current_timestamp - interval '1' hour
), silver_h AS (
  SELECT count(*) AS c FROM tickberg.silver_kis_tick_clean
  WHERE silver_ts >= current_timestamp - interval '1' hour
)
SELECT
  bronze_h.c AS bronze_rows,
  silver_h.c AS silver_rows,
  CAST(silver_h.c AS double) / NULLIF(bronze_h.c, 0) AS dedup_ratio
FROM bronze_h, silver_h;
```

- [ ] **Step 3: `code/health-queries/03_symbol_coverage.sql`**

```sql
-- 최근 15분 종목별 tick count. 3 종목 모두 보여야 정상.
SELECT
  symbol,
  count(*) AS tick_count,
  max(trade_ts_kst) AS last_trade,
  count(distinct date_trunc('minute', trade_ts_kst)) AS active_minutes
FROM tickberg.silver_kis_tick_clean
WHERE silver_ts >= current_timestamp - interval '15' minute
GROUP BY symbol
ORDER BY symbol;
```

- [ ] **Step 4: `code/health-queries/04_gold_partition_completeness.sql`**

```sql
-- 영업시간 hour 별 분봉 row 수 → 180 = 3 종목 × 60 분 (정상)
SELECT
  date_trunc('hour', ts_minute) AS hr,
  count(*) AS row_count,
  count(distinct symbol) AS symbol_count,
  180 AS expected_rows
FROM tickberg.gold_symbol_vwap_1m
WHERE ts_minute >= current_date AND ts_minute < current_date + interval '1' day
GROUP BY 1
ORDER BY 1;
```

- [ ] **Step 5: helper — `infra/scripts/run_health_query.sh`**

```bash
#!/usr/bin/env bash
set -euo pipefail
: "${AWS_PROFILE:=tickberg}"
: "${AWS_REGION:=ap-northeast-2}"
WG="tickberg-wg"
DB="tickberg"
RESULTS="s3://tickberg-lakehouse/athena-results/"
QUERY_FILE="${1:?usage: $0 <sql-file>}"

qid=$(aws --profile "$AWS_PROFILE" --region "$AWS_REGION" athena start-query-execution \
  --query-string "$(cat "$QUERY_FILE")" \
  --query-execution-context "Database=${DB}" \
  --work-group "$WG" \
  --result-configuration "OutputLocation=${RESULTS}" \
  --output text --query 'QueryExecutionId')

while :; do
  state=$(aws --profile "$AWS_PROFILE" --region "$AWS_REGION" athena get-query-execution \
    --query-execution-id "$qid" --output text --query 'QueryExecution.Status.State')
  case "$state" in
    SUCCEEDED) break;;
    FAILED|CANCELLED)
      aws --profile "$AWS_PROFILE" --region "$AWS_REGION" athena get-query-execution \
        --query-execution-id "$qid" --output text --query 'QueryExecution.Status.StateChangeReason'
      exit 1;;
    *) sleep 1;;
  esac
done

aws --profile "$AWS_PROFILE" --region "$AWS_REGION" athena get-query-results \
  --query-execution-id "$qid" --output table
```

- [ ] **Step 6: 검증 (사용자 승인 후 Athena scan 발생)**

```bash
chmod +x infra/scripts/run_health_query.sh
for q in code/health-queries/0[1-4]_*.sql; do
  echo "=== $q ==="
  bash infra/scripts/run_health_query.sh "$q"
done
```
Expected: 4개 모두 SUCCEEDED. 영업시간 외 = row_count 0 (정상).

- [ ] **Step 7: Commit**

```bash
git add code/health-queries/0[1-4]_*.sql infra/scripts/run_health_query.sh
git commit -m "feat(observability): health-queries 1-4 (freshness, dedup, coverage, completeness)"
```

---

### Task 26: Grafana dashboard — 1차 2 패널 + Prometheus alert rules

Spec §5.3.

**Files:**
- Create: `monitoring/grafana/dashboards/tickberg-1a.json`
- Create: `monitoring/prometheus/alerts.yml`
- Modify: `monitoring/prometheus/prometheus.yml` (alerts include)

- [ ] **Step 1: dashboard JSON — `monitoring/grafana/dashboards/tickberg-1a.json`**

```json
{
  "title": "tickberg — 1차 운영 패널",
  "uid": "tickberg-1a",
  "timezone": "Asia/Seoul",
  "schemaVersion": 39,
  "refresh": "30s",
  "time": {"from": "now-1h", "to": "now"},
  "panels": [
    {
      "id": 1,
      "type": "timeseries",
      "title": "Kafka topic kis.tick.raw consumer-group lag (spark-streaming-bronze)",
      "datasource": {"type": "prometheus", "uid": "prometheus"},
      "targets": [{
        "expr": "sum by (group) (kafka_consumergroup_lag{group=\"spark-streaming-bronze\"})",
        "legendFormat": "{{group}}"
      }],
      "thresholds": {"mode": "absolute", "steps": [
        {"color": "green", "value": null},
        {"color": "yellow", "value": 1000},
        {"color": "red", "value": 10000}
      ]},
      "gridPos": {"h": 8, "w": 12, "x": 0, "y": 0}
    },
    {
      "id": 2,
      "type": "timeseries",
      "title": "Spark Structured Streaming batch duration (s)",
      "datasource": {"type": "prometheus", "uid": "prometheus"},
      "targets": [{
        "expr": "max(spark_streaming_batch_duration_seconds)",
        "legendFormat": "batch_duration_s"
      }],
      "thresholds": {"mode": "absolute", "steps": [
        {"color": "green", "value": null},
        {"color": "yellow", "value": 30},
        {"color": "red", "value": 50}
      ]},
      "gridPos": {"h": 8, "w": 12, "x": 12, "y": 0}
    }
  ]
}
```

- [ ] **Step 2: alert rules — `monitoring/prometheus/alerts.yml`**

```yaml
groups:
  - name: tickberg-1a
    interval: 30s
    rules:
      - alert: KafkaLagHigh
        expr: sum by (group) (kafka_consumergroup_lag{group="spark-streaming-bronze"}) > 10000
        for: 2m
        labels: {severity: critical}
        annotations:
          summary: "Kafka consumer lag > 10K"
          description: "spark-streaming-bronze 가 1분 trigger 못 따라가는 중. T1 → T2 격리"

      - alert: SparkBatchDurationSlow
        expr: max(spark_streaming_batch_duration_seconds) > 50
        for: 3m
        labels: {severity: warning}
        annotations:
          summary: "Streaming batch > 50s"

      - alert: KisWebSocketDown
        expr: kis_ws_connected == 0
        for: 5m
        labels: {severity: critical}
        annotations:
          summary: "KIS WebSocket disconnected for 5m"
          description: "장 시간대 라면 종목 데이터 손실 중 (장 마감 후 = 무시 가능)"
```

- [ ] **Step 3: prometheus.yml 에 alerts include 추가**

`monitoring/prometheus/prometheus.yml` 의 `global:` 다음에 추가:

```yaml
rule_files:
  - /etc/prometheus/alerts.yml
```

그리고 compose의 prometheus 서비스 volumes 에 alerts.yml 마운트 추가:

```yaml
  prometheus:
    image: prom/prometheus:v2.52.0
    container_name: tickberg-prometheus
    volumes:
      - ../../monitoring/prometheus/prometheus.yml:/etc/prometheus/prometheus.yml:ro
      - ../../monitoring/prometheus/alerts.yml:/etc/prometheus/alerts.yml:ro
    ports: ["9090:9090"]
```

- [ ] **Step 4: 검증**

```bash
docker compose -f infra/docker/docker-compose.yml up -d --force-recreate prometheus grafana
sleep 5
curl -s http://localhost:9090/api/v1/rules | python3 -c "import json,sys;print(json.dumps(json.load(sys.stdin),indent=2))" | head -40
```
Expected: `KafkaLagHigh`, `SparkBatchDurationSlow`, `KisWebSocketDown` rule 출현.

Grafana http://localhost:3000 (admin/admin) → Dashboards → "tickberg — 1차 운영 패널" 자동 provisioning 확인.

- [ ] **Step 5: Commit**

```bash
git add monitoring/grafana/dashboards/tickberg-1a.json monitoring/prometheus/alerts.yml monitoring/prometheus/prometheus.yml infra/docker/docker-compose.yml
git commit -m "feat(observability): Grafana 1차 dashboard + Prometheus alert rules"
```

---

### Task 27: QuickSight 운영탭 + KPI탭 (수동 셋업 + runbook 기록)

QuickSight 는 IaC 가능하지만 (CloudFormation `AWS::QuickSight::Dashboard`) Phase 1 시간 budget 안에서는 수동 셋업이 빠름. 5/16 풍부화 단계에서 export JSON 보관.

**Files:**
- Create: `dashboard/quicksight/setup.md`
- Create: `dashboard/quicksight/.gitkeep`

- [ ] **Step 1: `dashboard/quicksight/setup.md` (runbook)**

````markdown
# QuickSight 1차 셋업 — 5/10 발표용

## 사전 조건
- Athena workgroup `tickberg-wg` 동작 중
- IAM 사용자 `tickberg-user` 가 QuickSight 콘솔 접근 가능 (별도 Author)
- Standard 또는 Enterprise edition (Phase 1 = Standard 1 Author)

## 데이터셋 (Athena)

QuickSight 콘솔 → Datasets → New dataset → Athena → workgroup `tickberg-wg`.

### Dataset 1: `bronze_kis_tick_raw`
- Database: `tickberg`
- Table: `bronze_kis_tick_raw`
- Refresh: SPICE 직접 query (Direct query 권장 — 작은 양)

### Dataset 2: `silver_kis_tick_clean`
- Database: `tickberg`
- Table: `silver_kis_tick_clean`
- Direct query

### Dataset 3: `gold_symbol_vwap_1m`
- Direct query

## 분석 (Analyses)

### KPI탭 — 비즈니스 (5/10 1차 = 1탭)
1. **VWAP 추세 (line)**: gold_symbol_vwap_1m, x=ts_minute, y=vwap, color=symbol, 오늘 영업시간 필터
2. **분봉 거래량 (bar)**: x=ts_minute, y=total_volume, color=symbol
3. **종목별 거래량 점유율 (pie)**: gold_symbol_vwap_1m → sum(total_volume) by symbol

### 운영탭 (3 viz, spec §5.4)

1. **Bronze Freshness KPI (numeric)**: bronze_kis_tick_raw → max(ingest_ts), 표시 = "N분 전". Custom calculated field:
   ```
   freshness_minutes = dateDiff(maxOver(ingest_ts, []), now(), 'MI')
   ```
   임계값 노란 5분, 빨간 10분.

2. **Symbol Coverage Bar (1h)**: silver_kis_tick_clean → 최근 1h 종목별 row count. 3 막대 (삼성전자/SK하이닉스/NAVER), 누락 시 즉시 발견.

3. **Silver throughput per 5min (24h)**: silver_kis_tick_clean → x=floor(silver_ts to 5min), y=count(*). 영업시간 패턴 시각화.

## 권한
- IAM 사용자에 QuickSightAccess + Athena workgroup `tickberg-wg` 권한 부여

## 발표용 공유
- KPI탭 = 임시 share link (만료 24h) 발표 직전 생성
- 운영탭 = QuickSight 콘솔 화면 공유

## 5/16 추가 (운영탭 풍부화)
- Iceberg snapshot count over time (Athena → silver/gold `.snapshots` view)
- Late arrival histogram (silver `silver_ts - trade_ts_kst`)
- Source coverage (DART 공시 ingest 추세, 신용정보원 last_updated)
````

- [ ] **Step 2: 사용자 승인 후 QuickSight 콘솔에서 수동 셋업 (10–20분)**

이 task는 manual. plan 단계에선 runbook만 commit. 발표 24h 전 (5/9 토) 까지 셋업 완료 + 스크린샷 보관.

- [ ] **Step 3: Commit**

```bash
git add dashboard/quicksight/
git commit -m "docs(dashboard): QuickSight 1A setup runbook"
```

---

### Task 28: 영업일 녹화 prep (5/8 금 영업시간 동안)

발표일 5/10 일 = 비영업일. spec §7.2 — 5/8 금 영업시간 5–10분 녹화 채택.

**Files:**
- Create: `docs/superpowers/demo-recording-1a.md`

- [ ] **Step 1: 5/8 영업일 가동 체크리스트 — `docs/superpowers/demo-recording-1a.md`**

````markdown
# 1차 발표용 녹화 (5/8 금 영업시간)

## 녹화 대상 시점
- 5/8 금 09:30 (개장 직후 burst) **또는** 14:30 (장 후반 안정)
- 5–10분 분량

## 가동 순서 (영업시간 시작 30분 전)
1. `docker compose -f infra/docker/docker-compose.yml up -d` — 모든 서비스 기동
2. `bash infra/scripts/kafka_create_topics.sh` (이미 있으면 skip)
3. `docker logs tickberg-kis-producer --tail 20` — `subscribed 3 symbols` 확인
4. Airflow UI (http://localhost:8080) → DAG `bronze_to_silver_kis`, `silver_to_gold_vwap` Unpause
5. Grafana (http://localhost:3000) → "tickberg — 1차 운영 패널" 열어둠

## 녹화 화면 (4분할 또는 순차)
- (A) Grafana — Kafka lag 0 근처 + Spark batch duration < 30s
- (B) Spark UI (http://localhost:4040) — 1분 trigger micro-batch
- (C) Airflow UI — 5분 cycle DAG run 성공 표시
- (D) Athena 쿼리 — `code/health-queries/03_symbol_coverage.sql` 실행 → 3 종목 각 ~수백 row

## 녹화 산출물
- `docs/superpowers/recordings/2026-05-08-1a.mp4` (커밋 X — 용량)
- 발표 슬라이드에 임베드 또는 외부 호스팅 (YouTube unlisted)
- 백업: 각 화면 스크린샷 8–10장 (`docs/superpowers/recordings/screenshots/`)

## 데이터 보존
- Bronze 5/8 데이터 = `s3://tickberg-lakehouse/bronze/kis_tick_raw/dt=2026-05-08/`
- Silver / Gold 5/8 데이터 = Glue Catalog 통해 5/10 발표 시 Athena 라이브 쿼리 가능
- **5/9 토 의 Compaction DAG 가 자동 실행되면서 5/8 데이터도 정리됨 — Iceberg time-travel 시연 시 snapshot 보존 확인 필요**

## 5/16 추가 녹화
- 5/14 목 영업시간 동일 절차로 재녹화 (DART/신용정보원/풍부화 운영탭 포함)
````

- [ ] **Step 2: Commit (실제 녹화는 5/8 영업시간 가동 후 별도 작업)**

```bash
git add docs/superpowers/demo-recording-1a.md
git commit -m "docs: 1차 발표용 영업일 녹화 runbook (5/8)"
```

---

### Task 29: 30분 전 smoke checklist + demo runbook + Phase 2 placeholder dir

Spec §6.9.

**Files:**
- Create: `docs/superpowers/demo-runbook-1a.md`
- Create: `infra/scripts/smoke_check.sh`
- Create: `code/pipelines/trading/.gitkeep` (Phase 2 placeholder, CLAUDE.md "Phase 2 deferred")

- [ ] **Step 1: smoke 스크립트 — `infra/scripts/smoke_check.sh`**

```bash
#!/usr/bin/env bash
set -euo pipefail
# 발표 30분 전 헬스체크 — 5분 안에 모든 항목 PASS 되어야 시연 안전

ok()  { printf "  \033[32mOK\033[0m  %s\n" "$1"; }
err() { printf "  \033[31mFAIL\033[0m  %s\n" "$1"; FAILED=1; }
FAILED=0

echo "[1] containers up"
for svc in tickberg-kafka tickberg-spark-master tickberg-spark-worker \
           tickberg-spark-streaming tickberg-airflow-scheduler \
           tickberg-airflow-web tickberg-prometheus tickberg-grafana \
           tickberg-kis-producer; do
  if [ "$(docker inspect -f '{{.State.Running}}' "$svc" 2>/dev/null || echo false)" = "true" ]; then
    ok "$svc"
  else
    err "$svc not running"
  fi
done

echo "[2] Kafka topic + recent messages"
docker exec tickberg-kafka kafka-topics.sh --bootstrap-server localhost:9092 \
  --describe --topic kis.tick.raw >/dev/null && ok "topic kis.tick.raw exists" || err "topic missing"

echo "[3] KIS producer health metrics"
WS=$(curl -s localhost:9100/metrics | grep '^kis_ws_connected ' | awk '{print $2}')
if [ "$WS" = "1.0" ] || [ "$WS" = "1" ]; then ok "kis_ws_connected=1"; else err "kis_ws_connected=$WS"; fi

echo "[4] Recent Bronze partition exists (today)"
TODAY=$(date +%Y-%m-%d)
COUNT=$(aws --profile "${AWS_PROFILE:-tickberg}" s3 ls "s3://tickberg-lakehouse/bronze/kis_tick_raw/dt=${TODAY}/" 2>/dev/null | wc -l)
if [ "$COUNT" -gt 0 ]; then ok "bronze partition dt=${TODAY} ($COUNT entries)"; else err "no bronze for today (영업일 외라면 이전 영업일 dt 확인)"; fi

echo "[5] Airflow DAGs unpaused"
for dag in bronze_to_silver_kis silver_to_gold_vwap dim_symbol_daily iceberg_compaction; do
  state=$(docker exec tickberg-airflow-scheduler airflow dags details "$dag" 2>/dev/null | grep -E '^is_paused' | awk '{print $3}')
  if [ "$state" = "False" ]; then ok "$dag unpaused"; else err "$dag paused or missing"; fi
done

echo "[6] Athena health-query smoke (1 query, scan ≤ 5GB)"
bash infra/scripts/run_health_query.sh code/health-queries/03_symbol_coverage.sql >/dev/null && ok "symbol_coverage query OK" || err "athena query failed"

echo "[7] Grafana dashboard reachable"
curl -sf http://localhost:3000/api/health >/dev/null && ok "grafana healthy" || err "grafana down"

if [ "$FAILED" = "0" ]; then
  echo
  printf "\033[32mALL CLEAR\033[0m — demo 안전\n"
  exit 0
else
  echo
  printf "\033[31m%d 실패\033[0m — fallback 결정 필요 (spec §7.4)\n" "$FAILED"
  exit 1
fi
```

- [ ] **Step 2: demo runbook — `docs/superpowers/demo-runbook-1a.md`**

````markdown
# 5/10 1차 발표 runbook

## T-30min smoke
```bash
bash infra/scripts/smoke_check.sh
```
모두 OK 면 진행. 1개라도 FAIL → spec §7.4 fallback 매트릭스 참조.

## 발표 흐름 (10분)
1. (1min) 동기·결정 — slide
2. (3min) 시연 — **녹화 영상 재생 (5/8 영업시간)** + Athena live 쿼리 1번
3. (2min) Iceberg ① MERGE dedup 시연 — `tests/test_silver_merge.py::test_duplicate_trade_uid_merged_to_one_row` 결과 + Athena `silver_kis_tick_clean` snapshot history (`SELECT * FROM "silver_kis_tick_clean$snapshots"`)
4. (2min) 운영 가시성 — Grafana 패널 2개 + Airflow UI 5분 cycle + 5분 헬스체크 narrative
5. (2min) 100x scale + Phase 2 로드맵

## 백업 (라이브 끊김 시)
- Athena 쿼리 스크린샷 4장 (`docs/superpowers/recordings/screenshots/`)
- Grafana 스냅샷 (Grafana → Share → Export snapshot, JSON 파일 보관)

## T-0 발표 직전
- VPN/방화벽으로 KIS WebSocket 차단되어 있지 않은지 확인 (1주일 전 한번 KIS 콘솔에서 IP 등록)
- AWS 콘솔 로그인된 brave 탭 1개 + Athena 쿼리 미리 입력 1개
````

- [ ] **Step 3: Phase 2 placeholder + chmod**

```bash
mkdir -p code/pipelines/trading
touch code/pipelines/trading/.gitkeep
chmod +x infra/scripts/smoke_check.sh
```

- [ ] **Step 4: smoke 스크립트 dry-run**

```bash
bash infra/scripts/smoke_check.sh || true
```
Expected: 영업시간 외라면 일부 FAIL (kis_ws_connected, bronze partition for today). 영업시간이면 ALL CLEAR.

- [ ] **Step 5: Commit**

```bash
git add infra/scripts/smoke_check.sh docs/superpowers/demo-runbook-1a.md code/pipelines/trading/.gitkeep
git commit -m "feat(demo): smoke check script + 1차 발표 runbook + Phase 2 placeholder"
```

---

### Milestone 1A 완료 게이트 (5/9 토 21:00 cutoff)

이 시점에 다음이 모두 동작해야 함 (spec §7.4 cutoff):

- [ ] `bash infra/scripts/smoke_check.sh` ALL CLEAR (영업시간 외라면 ws/bronze 항목 알고 있는 FAIL 만)
- [ ] `pytest -q tests/` 모두 PASS
- [ ] Athena 4 health query 모두 SUCCEEDED
- [ ] Grafana 2 패널 가시화 + Prometheus alert rule 3개 활성
- [ ] QuickSight 운영탭 3 viz + KPI탭 동작
- [ ] 5/8 영업시간 녹화 영상 확보 + 백업 스크린샷
- [ ] Iceberg MERGE dedup 시연 SQL 1번 실제 Athena 실행 검증
- [ ] `docs/superpowers/demo-runbook-1a.md` 한 번 따라 완주

**FAIL 항목 → spec §7.4 fallback 적용. 5/9 토 21:00 이후 코드 변경 0.**

---

## Milestone 1B — 5/14 자정 PPT 마감 + 5/16 토 최종 발표 풍부화 (5/11 월 ~ 5/14 목)

목표: 1A의 working E2E 위에 DART/신용정보원 통합 + 운영 가시성 풍부화 + Iceberg 매니지먼트 자동화 1개 추가 + Test 강화 + 100x design doc 정리. 5/13 수 21:00 = 모든 fallback 결정 종료.

---

### Task 30: DART API client + Bronze DDL + ingest DAG (5/11 월)

DART OpenDART API: 분기 재무제표 (`fnlttSinglAcntAll.json`) + 주요 공시 (`list.json`).  
Phase 1B = 일배치 06:00 KST — 영업일 전날 공시 list 수집.

**Files:**
- Create: `code/ddl/bronze/dart_disclosure_raw.sql`
- Create: `code/pipelines/bronze/dart_disclosure_ingest.py`
- Create: `tests/test_dart_client.py`
- Create: `infra/docker/kis-producer/src/dart_client.py` (재사용 위치 — 또는 `code/pipelines/bronze/_dart_client.py`)
- Create: `orchestration/dags/dart_ingest_daily.py`

- [ ] **Step 1: Bronze DDL — `code/ddl/bronze/dart_disclosure_raw.sql`**

```sql
CREATE EXTERNAL TABLE IF NOT EXISTS tickberg.bronze_dart_disclosure_raw (
  rcept_no    string,
  corp_code   string,
  corp_name   string,
  stock_code  string,
  report_nm   string,
  rcept_dt    string,
  flr_nm      string,
  rm          string,
  ingest_ts   timestamp
)
PARTITIONED BY (dt date)
STORED AS PARQUET
LOCATION 's3://tickberg-lakehouse/bronze/dart_disclosure_raw/'
TBLPROPERTIES (
  'projection.enabled'         = 'true',
  'projection.dt.type'         = 'date',
  'projection.dt.range'        = '2026-05-01,NOW',
  'projection.dt.format'       = 'yyyy-MM-dd',
  'projection.dt.interval'     = '1',
  'projection.dt.interval.unit'= 'DAYS',
  'storage.location.template'  = 's3://tickberg-lakehouse/bronze/dart_disclosure_raw/dt=${dt}/'
);
```

- [ ] **Step 2: 실패 테스트 — `tests/test_dart_client.py`**

```python
from unittest.mock import MagicMock

import pytest

from code.pipelines.bronze.dart_client import DartClient, DartError


def test_fetch_disclosures_filters_by_corp_codes():
    http = MagicMock()
    http.get.return_value.json.return_value = {
        "status": "000",
        "list": [
            {"rcept_no": "1", "stock_code": "005930", "report_nm": "분기보고서", "rcept_dt": "20260508"},
            {"rcept_no": "2", "stock_code": "999999", "report_nm": "기타", "rcept_dt": "20260508"},
        ],
    }
    http.get.return_value.raise_for_status = lambda: None
    c = DartClient(api_key="K", http=http)

    rows = c.fetch_disclosures(business_date="20260508", stock_codes={"005930"})

    assert len(rows) == 1
    assert rows[0]["stock_code"] == "005930"


def test_fetch_disclosures_raises_on_api_error():
    http = MagicMock()
    http.get.return_value.json.return_value = {"status": "010", "message": "API key invalid"}
    http.get.return_value.raise_for_status = lambda: None
    c = DartClient(api_key="K", http=http)
    with pytest.raises(DartError):
        c.fetch_disclosures(business_date="20260508", stock_codes={"005930"})
```

- [ ] **Step 3: Implement — `code/pipelines/bronze/dart_client.py`**

```python
"""DART OpenDART list.json wrapper."""
from __future__ import annotations

from collections.abc import Iterable
from typing import Any


class DartError(RuntimeError):
    pass


class DartClient:
    BASE = "https://opendart.fss.or.kr/api"

    def __init__(self, *, api_key: str, http):
        self._api_key = api_key
        self._http = http

    def fetch_disclosures(self, *, business_date: str, stock_codes: Iterable[str]) -> list[dict[str, Any]]:
        """Fetch all disclosures for `business_date` (YYYYMMDD), filter by stock_codes."""
        wanted = set(stock_codes)
        all_rows: list[dict[str, Any]] = []
        page = 1
        while True:
            r = self._http.get(
                f"{self.BASE}/list.json",
                params={
                    "crtfc_key": self._api_key,
                    "bgn_de": business_date, "end_de": business_date,
                    "page_no": page, "page_count": 100,
                },
                timeout=15,
            )
            r.raise_for_status()
            body = r.json()
            if body.get("status") not in ("000", "013"):  # 013 = no data
                raise DartError(f"DART error status={body.get('status')} msg={body.get('message')}")
            for row in body.get("list", []):
                if row.get("stock_code") in wanted:
                    all_rows.append(row)
            if body.get("page_no", page) >= body.get("total_page", page):
                break
            page += 1
        return all_rows
```

- [ ] **Step 4: ingest job — `code/pipelines/bronze/dart_disclosure_ingest.py`**

```python
"""Daily DART disclosure → Bronze Parquet (06:00 KST)."""
from __future__ import annotations

import argparse
import os
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import requests
from pyspark.sql import Row, SparkSession

from code.pipelines.bronze.dart_client import DartClient

KST = ZoneInfo("Asia/Seoul")


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--business-date", default=None, help="YYYYMMDD; default = yesterday KST")
    p.add_argument("--symbols", default=os.environ.get("KIS_SYMBOLS", "005930,000660,035420"))
    p.add_argument("--bronze-table", default="glue.tickberg.bronze_dart_disclosure_raw")
    args = p.parse_args()

    biz = args.business_date or (datetime.now(KST) - timedelta(days=1)).strftime("%Y%m%d")
    symbols = [s.strip() for s in args.symbols.split(",") if s.strip()]

    client = DartClient(api_key=os.environ["DART_API_KEY"], http=requests.Session())
    rows = client.fetch_disclosures(business_date=biz, stock_codes=symbols)

    spark = SparkSession.builder.appName("dart_disclosure_ingest").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    now = datetime.now(KST).replace(tzinfo=None)
    spark_rows = [
        Row(
            rcept_no=r.get("rcept_no", ""), corp_code=r.get("corp_code", ""),
            corp_name=r.get("corp_name", ""), stock_code=r.get("stock_code", ""),
            report_nm=r.get("report_nm", ""), rcept_dt=r.get("rcept_dt", ""),
            flr_nm=r.get("flr_nm", ""), rm=r.get("rm", ""),
            ingest_ts=now, dt=datetime.strptime(biz, "%Y%m%d").date(),
        )
        for r in rows
    ]
    if not spark_rows:
        print("no DART rows for this business date")
        return
    df = spark.createDataFrame(spark_rows)
    (df.write.mode("append").format("parquet")
       .partitionBy("dt")
       .save(f"s3a://{os.environ.get('S3_BUCKET','tickberg-lakehouse')}/bronze/dart_disclosure_raw/"))
    print(f"wrote {len(spark_rows)} DART rows for {biz}")


if __name__ == "__main__":
    main()
```

- [ ] **Step 5: DAG — `orchestration/dags/dart_ingest_daily.py`**

```python
from __future__ import annotations
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from _common import spark_submit_command

with DAG(
    dag_id="dart_ingest_daily",
    default_args={"owner": "tickberg", "retries": 2,
                  "retry_delay": timedelta(minutes=5),
                  "email_on_failure": True},
    schedule="0 6 * * MON-FRI",
    start_date=datetime(2026, 5, 11, 6, 0),
    catchup=False,
    max_active_runs=1,
    tags=["bronze", "dart", "after-hours"],
) as dag:
    BashOperator(
        task_id="dart_disclosure_ingest",
        bash_command=spark_submit_command(
            "/opt/spark/code/pipelines/bronze/dart_disclosure_ingest.py",
            "--symbols", "005930,000660,035420",
        ),
        env={"DART_API_KEY": "{{ var.value.DART_API_KEY }}"},
    )
```

- [ ] **Step 6: 실행 + Run + Commit**

```bash
docker exec tickberg-airflow-scheduler airflow variables set DART_API_KEY "$DART_API_KEY"
bash infra/scripts/run_ddl.sh
pytest tests/test_dart_client.py -v
git add code/ddl/bronze/dart_disclosure_raw.sql code/pipelines/bronze/dart_client.py code/pipelines/bronze/dart_disclosure_ingest.py orchestration/dags/dart_ingest_daily.py tests/test_dart_client.py
git commit -m "feat(dart): DART disclosure daily ingest (06:00 KST)"
```

---

### Task 31: DART Silver MERGE (`silver.dart_disclosure_clean`)

종목별 dedup + 1차 정제. Iceberg MERGE INTO. cut 우선순위 D4 — 시간 부족 시 Bronze + Athena 직접 쿼리로 단축 가능.

**Files:**
- Create: `code/ddl/silver/dart_disclosure_clean.sql`
- Create: `code/pipelines/silver/dart_silver_merge.py`
- Create: `tests/test_dart_silver_merge.py`

- [ ] **Step 1: DDL — `code/ddl/silver/dart_disclosure_clean.sql`**

```sql
CREATE TABLE IF NOT EXISTS tickberg.silver_dart_disclosure_clean (
  rcept_no    string,
  stock_code  string,
  corp_name   string,
  report_nm   string,
  report_kind string,         -- 분기/반기/사업/기타
  rcept_ts    timestamp,
  flr_nm      string,
  silver_ts   timestamp
)
PARTITIONED BY (day(rcept_ts))
LOCATION 's3://tickberg-lakehouse/silver/dart_disclosure_clean/'
TBLPROPERTIES ('table_type'='ICEBERG', 'format'='parquet', 'format-version'='2');
```

- [ ] **Step 2: 실패 테스트 — `tests/test_dart_silver_merge.py`**

```python
from datetime import datetime
import pytest
from pyspark.sql import Row
from code.pipelines.silver.dart_silver_merge import classify_report_kind, merge_dart


@pytest.mark.parametrize("name,kind", [
    ("분기보고서", "분기"),
    ("반기보고서", "반기"),
    ("사업보고서", "사업"),
    ("주요사항보고서(유상증자결정)", "기타"),
])
def test_classify_report_kind(name, kind):
    assert classify_report_kind(name) == kind
```

- [ ] **Step 3: Implement — `code/pipelines/silver/dart_silver_merge.py`**

```python
from __future__ import annotations
import argparse
from datetime import datetime
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


def classify_report_kind(report_nm: str) -> str:
    if not report_nm: return "기타"
    if "분기보고서" in report_nm: return "분기"
    if "반기보고서" in report_nm: return "반기"
    if "사업보고서" in report_nm: return "사업"
    return "기타"


_classify_udf = F.udf(classify_report_kind)


def merge_dart(spark: SparkSession, *, bronze_df: DataFrame, silver_table: str) -> int:
    enriched = (
        bronze_df
        .withColumn("rcept_ts", F.to_timestamp(F.col("rcept_dt"), "yyyyMMdd"))
        .withColumn("report_kind", _classify_udf(F.col("report_nm")))
        .withColumn("silver_ts", F.current_timestamp())
        .select("rcept_no", "stock_code", "corp_name", "report_nm", "report_kind",
                "rcept_ts", "flr_nm", "silver_ts")
    )
    enriched.createOrReplaceTempView("_dart_stage")
    spark.sql(f"""
      MERGE INTO {silver_table} t
      USING (SELECT * FROM _dart_stage) s
      ON t.rcept_no = s.rcept_no
      WHEN NOT MATCHED THEN INSERT *
    """)
    return enriched.count()


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--bronze-table", default="glue.tickberg.bronze_dart_disclosure_raw")
    p.add_argument("--silver-table", default="glue.tickberg.silver_dart_disclosure_clean")
    p.add_argument("--days-back", type=int, default=2)
    args = p.parse_args()
    spark = SparkSession.builder.appName("dart_silver_merge").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    spark.sparkContext.setLocalProperty("spark.scheduler.pool", "batch_pool")
    bronze = spark.table(args.bronze_table).where(
        F.col("dt") >= F.current_date() - F.expr(f"INTERVAL {args.days_back} DAYS")
    )
    n = merge_dart(spark, bronze_df=bronze, silver_table=args.silver_table)
    print(f"dart silver merged {n} rows")


if __name__ == "__main__":
    main()
```

- [ ] **Step 4: DAG 추가 — `orchestration/dags/dart_ingest_daily.py` 에 task 추가 (chained)**

`BashOperator(task_id="dart_disclosure_ingest", ...)` 뒤에 추가:

```python
    silver_merge = BashOperator(
        task_id="dart_silver_merge",
        bash_command=spark_submit_command(
            "/opt/spark/code/pipelines/silver/dart_silver_merge.py",
            "--days-back", "2",
        ),
    )
    # at module bottom under the with-block:
    dag.task_dict["dart_disclosure_ingest"] >> silver_merge
```

- [ ] **Step 5: 실행 + Commit**

```bash
bash infra/scripts/run_ddl.sh
pytest tests/test_dart_silver_merge.py -v
git add code/ddl/silver/dart_disclosure_clean.sql code/pipelines/silver/dart_silver_merge.py tests/test_dart_silver_merge.py orchestration/dags/dart_ingest_daily.py
git commit -m "feat(dart): silver dart_disclosure_clean MERGE chained to ingest DAG"
```

---

### Task 32: 신용정보원 xlsx 파서 + Bronze ingest (수동 trigger)

월별 증강 데이터. Phase 1B = Bronze 까지만. cut 우선순위 D3 — 시간 부족 시 컷.

**Files:**
- Create: `code/ddl/bronze/credit_info_raw.sql`
- Create: `code/pipelines/bronze/credit_info_ingest.py`
- Create: `tests/test_credit_info_parser.py`
- Create: `orchestration/dags/credit_info_monthly.py`

- [ ] **Step 1: DDL — `code/ddl/bronze/credit_info_raw.sql`**

```sql
CREATE EXTERNAL TABLE IF NOT EXISTS tickberg.bronze_credit_info_raw (
  symbol           string,
  metric_name      string,
  metric_value     string,
  source_filename  string,
  ingest_ts        timestamp
)
PARTITIONED BY (yyyymm string)
STORED AS PARQUET
LOCATION 's3://tickberg-lakehouse/bronze/credit_info_raw/'
TBLPROPERTIES (
  'projection.enabled'        = 'true',
  'projection.yyyymm.type'    = 'integer',
  'projection.yyyymm.range'   = '202601,202612',
  'projection.yyyymm.digits'  = '6',
  'storage.location.template' = 's3://tickberg-lakehouse/bronze/credit_info_raw/yyyymm=${yyyymm}/'
);
```

- [ ] **Step 2: 실패 테스트 — `tests/test_credit_info_parser.py`**

```python
from pathlib import Path
import pandas as pd
import pytest
from code.pipelines.bronze.credit_info_ingest import parse_xlsx_to_long


def test_xlsx_long_format(tmp_path: Path):
    df = pd.DataFrame({
        "symbol": ["005930", "000660"],
        "신용잔고율": [3.2, 4.1],
        "공매도비중": [1.1, 2.5],
    })
    f = tmp_path / "credit_202604.xlsx"
    df.to_excel(f, index=False)

    rows = parse_xlsx_to_long(f)
    pairs = {(r["symbol"], r["metric_name"]) for r in rows}
    assert ("005930", "신용잔고율") in pairs
    assert ("000660", "공매도비중") in pairs
    assert all(r["source_filename"] == "credit_202604.xlsx" for r in rows)
```

- [ ] **Step 3: Implement — `code/pipelines/bronze/credit_info_ingest.py`**

```python
"""신용정보원 월별 xlsx → Bronze Parquet long format."""
from __future__ import annotations

import argparse
import os
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd
from pyspark.sql import SparkSession

KST = ZoneInfo("Asia/Seoul")


def parse_xlsx_to_long(xlsx_path: Path) -> list[dict]:
    df = pd.read_excel(xlsx_path)
    if "symbol" not in df.columns:
        raise ValueError("xlsx must have 'symbol' column")
    metric_cols = [c for c in df.columns if c != "symbol"]
    long = df.melt(id_vars=["symbol"], value_vars=metric_cols,
                   var_name="metric_name", value_name="metric_value")
    long["source_filename"] = xlsx_path.name
    return long.to_dict(orient="records")


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--xlsx-path", required=True)
    p.add_argument("--yyyymm", required=True, help="e.g. 202604")
    args = p.parse_args()

    rows = parse_xlsx_to_long(Path(args.xlsx_path))
    now = datetime.now(KST).replace(tzinfo=None)
    for r in rows:
        r["ingest_ts"] = now
        r["yyyymm"] = args.yyyymm
        r["metric_value"] = str(r["metric_value"])

    spark = SparkSession.builder.appName("credit_info_ingest").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    df = spark.createDataFrame(rows)
    (df.write.mode("append").format("parquet")
       .partitionBy("yyyymm")
       .save(f"s3a://{os.environ.get('S3_BUCKET','tickberg-lakehouse')}/bronze/credit_info_raw/"))
    print(f"wrote {len(rows)} credit-info rows for {args.yyyymm}")


if __name__ == "__main__":
    main()
```

- [ ] **Step 4: DAG (manual / monthly trigger) — `orchestration/dags/credit_info_monthly.py`**

```python
from __future__ import annotations
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from _common import spark_submit_command

with DAG(
    dag_id="credit_info_monthly",
    default_args={"owner": "tickberg", "retries": 1, "retry_delay": timedelta(minutes=10)},
    schedule=None,        # 수동 trigger
    start_date=datetime(2026, 5, 1),
    catchup=False,
    max_active_runs=1,
    tags=["bronze", "credit-info", "manual"],
) as dag:
    BashOperator(
        task_id="ingest",
        bash_command=spark_submit_command(
            "/opt/spark/code/pipelines/bronze/credit_info_ingest.py",
            "--xlsx-path", "{{ dag_run.conf['xlsx_path'] }}",
            "--yyyymm", "{{ dag_run.conf['yyyymm'] }}",
        ),
    )
```

- [ ] **Step 5: 실행 + Commit**

```bash
bash infra/scripts/run_ddl.sh
pip install pandas openpyxl
pytest tests/test_credit_info_parser.py -v
git add code/ddl/bronze/credit_info_raw.sql code/pipelines/bronze/credit_info_ingest.py orchestration/dags/credit_info_monthly.py tests/test_credit_info_parser.py
git commit -m "feat(credit-info): monthly xlsx → bronze long format ingest"
```

---

### Task 33: `expire_snapshots` DAG (Iceberg 매니지먼트 자동화 #2)

Spec §4.2 — `0 19 * * SUN`, 30일 retention.

**Files:**
- Create: `code/pipelines/silver/expire_snapshots.py`
- Create: `orchestration/dags/expire_snapshots.py`

- [ ] **Step 1: job — `code/pipelines/silver/expire_snapshots.py`**

```python
"""Expire snapshots older than retention_days for Silver/Gold tables."""
from __future__ import annotations

import argparse
from datetime import datetime, timedelta

from pyspark.sql import SparkSession


def expire(spark: SparkSession, *, table: str, retention_days: int) -> dict:
    catalog = table.split(".")[0]
    qualified = ".".join(table.split(".")[1:])
    older_than = (datetime.now() - timedelta(days=retention_days)).isoformat()
    sql = f"""
      CALL {catalog}.system.expire_snapshots(
        table => '{qualified}',
        older_than => TIMESTAMP '{older_than}',
        retain_last => 5
      )
    """
    return (spark.sql(sql).collect()[0].asDict()
            if spark.sql(sql).count() else {})


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--tables", nargs="+", default=[
        "glue.tickberg.silver_kis_tick_clean",
        "glue.tickberg.silver_dart_disclosure_clean",
        "glue.tickberg.gold_symbol_vwap_1m",
    ])
    p.add_argument("--retention-days", type=int, default=30)
    args = p.parse_args()
    spark = SparkSession.builder.appName("expire_snapshots").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    spark.sparkContext.setLocalProperty("spark.scheduler.pool", "batch_pool")
    for t in args.tables:
        try:
            r = expire(spark, table=t, retention_days=args.retention_days)
            print(f"expired {t}: {r}")
        except Exception as e:  # noqa: BLE001
            print(f"WARN expire failed {t}: {e}")


if __name__ == "__main__":
    main()
```

- [ ] **Step 2: DAG — `orchestration/dags/expire_snapshots.py`**

```python
from __future__ import annotations
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from _common import spark_submit_command

with DAG(
    dag_id="expire_snapshots",
    default_args={"owner": "tickberg", "retries": 1,
                  "retry_delay": timedelta(minutes=10), "email_on_failure": True},
    schedule="0 19 * * SUN",
    start_date=datetime(2026, 5, 17, 19, 0),
    catchup=False,
    max_active_runs=1,
    tags=["maintenance", "after-hours"],
) as dag:
    BashOperator(
        task_id="expire_snapshots",
        bash_command=spark_submit_command(
            "/opt/spark/code/pipelines/silver/expire_snapshots.py",
            "--retention-days", "30",
        ),
    )
```

- [ ] **Step 3: Commit**

```bash
git add code/pipelines/silver/expire_snapshots.py orchestration/dags/expire_snapshots.py
git commit -m "feat(maintenance): weekly expire_snapshots DAG (Sun 19:00, 30d retention)"
```

---

### Task 34: Grafana 풍부화 — 추가 4 패널

Spec §5.3 "5/16 추가": KIS producer message rate, ws_connected, batch lag (DAG별), Athena query latency.

**Files:**
- Create: `monitoring/grafana/dashboards/tickberg-1b.json`
- Modify: `monitoring/prometheus/alerts.yml` (rule 추가)

- [ ] **Step 1: dashboard — `monitoring/grafana/dashboards/tickberg-1b.json`**

```json
{
  "title": "tickberg — 1B 풍부화 패널",
  "uid": "tickberg-1b",
  "timezone": "Asia/Seoul",
  "schemaVersion": 39,
  "refresh": "30s",
  "time": {"from": "now-3h", "to": "now"},
  "panels": [
    {
      "id": 10, "type": "timeseries",
      "title": "KIS producer message rate per symbol (1m)",
      "datasource": {"type": "prometheus", "uid": "prometheus"},
      "targets": [{
        "expr": "sum by (symbol) (rate(kis_messages_published_total[1m]))",
        "legendFormat": "{{symbol}}"
      }],
      "gridPos": {"h": 8, "w": 12, "x": 0, "y": 0}
    },
    {
      "id": 11, "type": "stat",
      "title": "kis_ws_connected",
      "datasource": {"type": "prometheus", "uid": "prometheus"},
      "targets": [{"expr": "kis_ws_connected"}],
      "fieldConfig": {"defaults": {"thresholds": {"steps": [
        {"color": "red", "value": null}, {"color": "green", "value": 1}]}}},
      "gridPos": {"h": 4, "w": 6, "x": 12, "y": 0}
    },
    {
      "id": 12, "type": "stat",
      "title": "Token refresh failures (24h)",
      "datasource": {"type": "prometheus", "uid": "prometheus"},
      "targets": [{"expr": "increase(kis_token_refresh_failures_total[24h])"}],
      "gridPos": {"h": 4, "w": 6, "x": 18, "y": 0}
    },
    {
      "id": 13, "type": "timeseries",
      "title": "Airflow DAG run duration p95 (s)",
      "datasource": {"type": "prometheus", "uid": "prometheus"},
      "targets": [{
        "expr": "histogram_quantile(0.95, sum by (dag_id, le) (rate(airflow_dag_run_duration_bucket[15m])))",
        "legendFormat": "{{dag_id}}"
      }],
      "gridPos": {"h": 8, "w": 12, "x": 0, "y": 8}
    },
    {
      "id": 14, "type": "stat",
      "title": "Parse errors (1h)",
      "datasource": {"type": "prometheus", "uid": "prometheus"},
      "targets": [{"expr": "increase(kis_parse_errors_total[1h])"}],
      "fieldConfig": {"defaults": {"thresholds": {"steps": [
        {"color": "green", "value": null}, {"color": "yellow", "value": 5},
        {"color": "red", "value": 50}]}}},
      "gridPos": {"h": 4, "w": 6, "x": 12, "y": 8}
    }
  ]
}
```

- [ ] **Step 2: alert rules 추가 (`monitoring/prometheus/alerts.yml` 의 rules: 끝에 append)**

```yaml
      - alert: KisTokenRefreshFailing
        expr: increase(kis_token_refresh_failures_total[1h]) > 3
        for: 5m
        labels: {severity: critical}
        annotations:
          summary: "Token refresh failed > 3 times in 1h"
          description: "spec §6.1.1 04:30 cutoff 위험 — 수동 재발급 검토"

      - alert: KisParseErrorBurst
        expr: increase(kis_parse_errors_total[10m]) > 100
        for: 5m
        labels: {severity: warning}
        annotations:
          summary: "Parse errors > 100 in 10m — KIS schema 변경 의심"
```

- [ ] **Step 3: 검증 + Commit**

```bash
docker compose -f infra/docker/docker-compose.yml restart prometheus grafana
git add monitoring/grafana/dashboards/tickberg-1b.json monitoring/prometheus/alerts.yml
git commit -m "feat(observability): Grafana 1B panels + token/parse alert rules"
```

---

### Task 35: Health queries 추가 3개 (05/06/07)

Spec §5.5 "5/16 추가".

**Files:**
- Create: `code/health-queries/05_late_arrival_distribution.sql`
- Create: `code/health-queries/06_iceberg_snapshot_growth.sql`
- Create: `code/health-queries/07_compaction_file_reduction.sql`

- [ ] **Step 1: `05_late_arrival_distribution.sql`**

```sql
-- silver_ts - trade_ts_kst 분포: 100ms / 1s / 10s / >10s 버킷
WITH t AS (
  SELECT date_diff('second', trade_ts_kst, silver_ts) AS lag_s
  FROM tickberg.silver_kis_tick_clean
  WHERE silver_ts >= current_timestamp - interval '1' day
)
SELECT
  CASE
    WHEN lag_s < 1   THEN '01_under_1s'
    WHEN lag_s < 10  THEN '02_1_to_10s'
    WHEN lag_s < 60  THEN '03_10_to_60s'
    WHEN lag_s < 600 THEN '04_60s_to_10m'
    ELSE '05_over_10m'
  END AS bucket,
  count(*) AS row_count
FROM t
GROUP BY 1
ORDER BY 1;
```

- [ ] **Step 2: `06_iceberg_snapshot_growth.sql`**

```sql
SELECT
  'silver_kis_tick_clean' AS table_name,
  count(*) AS snapshot_count,
  min(committed_at) AS oldest,
  max(committed_at) AS newest
FROM "tickberg"."silver_kis_tick_clean$snapshots"
UNION ALL
SELECT 'gold_symbol_vwap_1m',
       count(*), min(committed_at), max(committed_at)
FROM "tickberg"."gold_symbol_vwap_1m$snapshots";
```

- [ ] **Step 3: `07_compaction_file_reduction.sql`**

```sql
-- 최근 24h Compaction 효과: 파일 수 / 평균 크기 / 총 크기
WITH f AS (
  SELECT count(*) AS file_count,
         sum(file_size_in_bytes) AS total_bytes,
         avg(file_size_in_bytes) AS avg_bytes
  FROM "tickberg"."silver_kis_tick_clean$files"
)
SELECT
  file_count,
  round(total_bytes / 1024.0 / 1024.0, 2) AS total_mb,
  round(avg_bytes  / 1024.0 / 1024.0, 2) AS avg_mb,
  CASE
    WHEN avg_bytes >= 256*1024*1024 THEN 'OK (>=256MB)'
    WHEN avg_bytes >= 64*1024*1024  THEN 'WARN (Compaction 후 정상)'
    ELSE 'BAD small-files'
  END AS verdict
FROM f;
```

- [ ] **Step 4: 검증 + Commit**

```bash
for q in code/health-queries/0[567]_*.sql; do bash infra/scripts/run_health_query.sh "$q"; done
git add code/health-queries/0[567]_*.sql
git commit -m "feat(observability): health-queries 5-7 (late arrival, snapshots, compaction effect)"
```

---

### Task 36: QuickSight 운영탭 풍부화 (3 viz 추가)

Spec §5.4 "5/16 추가": Iceberg snapshot count over time, late arrival histogram, source coverage.

**Files:**
- Modify: `dashboard/quicksight/setup.md` (5/16 섹션 구체화)

- [ ] **Step 1: `dashboard/quicksight/setup.md` 의 "5/16 추가" 섹션을 다음으로 교체**

````markdown
## 5/16 운영탭 풍부화 (3 viz 추가)

### viz 4: Iceberg snapshot count (line)
- Athena dataset: `SELECT date_trunc('day', committed_at) AS day, count(*) AS snapshot_count FROM "tickberg"."silver_kis_tick_clean$snapshots" GROUP BY 1`
- y=snapshot_count, x=day
- 임계값 일별 200 초과 시 노란 (compaction 미동작 의심)

### viz 5: Late arrival histogram (bar)
- Athena dataset: `code/health-queries/05_late_arrival_distribution.sql` 결과 저장
- x=bucket, y=row_count

### viz 6: Source coverage (table)
- Athena dataset:
  ```sql
  SELECT 'kis_tick' AS source, max(silver_ts) AS last_silver
  FROM tickberg.silver_kis_tick_clean
  UNION ALL
  SELECT 'dart_disclosure', max(silver_ts) FROM tickberg.silver_dart_disclosure_clean
  UNION ALL
  SELECT 'credit_info', max(ingest_ts) FROM tickberg.bronze_credit_info_raw;
  ```
- 표시 형태 = 소스별 마지막 적재 시점, 24h 이상 lag 빨간 강조
````

- [ ] **Step 2: Commit**

```bash
git add dashboard/quicksight/setup.md
git commit -m "docs(quicksight): 1B 운영탭 풍부화 — snapshot/late-arrival/source-coverage"
```

---

### Task 37: Spark Streaming 통합 test (testcontainers Kafka + local Spark)

Spec §7.3.1. Kafka topic 에 메시지 publish → 1분 trigger 후 local 디렉토리에 Parquet 적재 확인.

**Files:**
- Create: `tests/test_streaming_integration.py`
- Modify: `pyproject.toml` (testcontainers 의존성 추가)

- [ ] **Step 1: `tests/test_streaming_integration.py`**

```python
"""End-to-end streaming integration test (testcontainers Kafka + local Spark).

KafkaContainer + bronze_kis_tick_streaming.build_query 의 sink 를 local 경로로 override.
영업시간 dependency 없음.
"""
from __future__ import annotations

import json
import os
import time
from pathlib import Path

import pytest
from kafka import KafkaProducer
from pyspark.sql import SparkSession
from testcontainers.kafka import KafkaContainer


@pytest.fixture(scope="module")
def kafka():
    with KafkaContainer("confluentinc/cp-kafka:7.6.0") as k:
        yield k


def test_kafka_to_parquet_micro_batch(kafka, tmp_path: Path, monkeypatch):
    bootstrap = kafka.get_bootstrap_server()

    out_dir = tmp_path / "bronze"
    ckpt_dir = tmp_path / "checkpoint"

    monkeypatch.setenv("KAFKA_BOOTSTRAP", bootstrap)
    monkeypatch.setenv("KAFKA_TOPIC_TICK", "kis.tick.raw")

    # publish 5 messages
    p = KafkaProducer(bootstrap_servers=bootstrap,
                      value_serializer=lambda v: json.dumps(v).encode())
    for i in range(5):
        p.send("kis.tick.raw", {
            "symbol": "005930", "trade_ts_kst": "2026-05-08T09:30:0%d" % i,
            "price": "72500", "volume": 100, "trade_side": "+",
            "best_ask_price": "72500", "best_bid_price": "72400",
            "cum_volume": 1000+i, "cum_amount": 100000000+i, "raw_payload": "test",
        })
    p.flush()

    spark = (SparkSession.builder.appName("streaming-it").master("local[2]")
             .config("spark.jars.packages",
                     "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1")
             .config("spark.ui.enabled", "false")
             .config("spark.sql.session.timeZone", "Asia/Seoul")
             .getOrCreate())

    from pipelines.bronze import bronze_kis_tick_streaming as m
    m.OUTPUT_PATH = str(out_dir)
    m.CHECKPOINT_PATH = str(ckpt_dir)
    m.BOOTSTRAP = bootstrap

    q = m.build_query(spark)
    deadline = time.time() + 90
    while time.time() < deadline:
        if any(out_dir.rglob("*.parquet")):
            break
        time.sleep(2)
    q.stop()

    df = spark.read.parquet(str(out_dir))
    assert df.count() == 5
    assert df.select("symbol").distinct().collect()[0].symbol == "005930"
    spark.stop()
```

- [ ] **Step 2: 의존성 추가 — `pyproject.toml` 에 dev extras (또는 직접 install)**

```bash
pip install kafka-python testcontainers[kafka]
pytest tests/test_streaming_integration.py -v --tb=short
```
Expected: PASS (90초 안에 micro-batch 1회 완료).

cut 우선순위 D2: 시간 부족 시 이 task 컷 가능. 5/14 PPT 마감 후 5/15 α 작업으로 미룬다.

- [ ] **Step 3: Commit**

```bash
git add tests/test_streaming_integration.py pyproject.toml
git commit -m "test(streaming): integration test with testcontainers Kafka + Spark"
```

---

### Task 38: E2E replay 검증 — Bronze 샘플 → Silver/Gold 기대값

Spec §7.3.1. cut 우선순위 D2. local Spark 만으로 Bronze→Silver→Gold pipeline assert.

**Files:**
- Create: `tests/test_e2e_replay.py`
- Create: `tests/fixtures/bronze_sample.json`

- [ ] **Step 1: fixture — `tests/fixtures/bronze_sample.json`**

```json
[
  {"ingest_ts": "2026-05-08T00:30:05", "kafka_partition": 0, "kafka_offset": 1,
   "symbol": "005930", "trade_ts_kst": "2026-05-08T09:30:01",
   "price": "72500", "volume": 100, "cum_volume": 1, "cum_amount": 7250000,
   "trade_side": "+", "best_ask_price": "72500", "best_bid_price": "72400",
   "raw_payload": "..."},
  {"ingest_ts": "2026-05-08T00:30:05", "kafka_partition": 0, "kafka_offset": 2,
   "symbol": "005930", "trade_ts_kst": "2026-05-08T09:30:02",
   "price": "72600", "volume": 200, "cum_volume": 2, "cum_amount": 21770000,
   "trade_side": "+", "best_ask_price": "72600", "best_bid_price": "72500",
   "raw_payload": "..."},
  {"ingest_ts": "2026-05-08T00:30:06", "kafka_partition": 0, "kafka_offset": 3,
   "symbol": "005930", "trade_ts_kst": "2026-05-08T09:30:01",
   "price": "72500", "volume": 100, "cum_volume": 1, "cum_amount": 7250000,
   "trade_side": "+", "best_ask_price": "72500", "best_bid_price": "72400",
   "raw_payload": "..."}
]
```

- [ ] **Step 2: 테스트 — `tests/test_e2e_replay.py`**

```python
"""E2E replay: load Bronze fixture → MERGE Silver → OVERWRITE Gold → assert."""
from __future__ import annotations

import json
from datetime import datetime
from decimal import Decimal
from pathlib import Path

import pytest
from pyspark.sql.types import (DecimalType, IntegerType, LongType, StringType,
                                StructField, StructType, TimestampType)

from pipelines.silver.bronze_to_silver_kis_tick import merge_bronze_into_silver
from pipelines.gold.silver_to_gold_vwap import compute_vwap_for_hour
from tests.spark_fixtures import spark, warehouse_dir   # noqa: F401


_BRONZE_SCHEMA = StructType([
    StructField("ingest_ts", TimestampType()),
    StructField("kafka_partition", IntegerType()),
    StructField("kafka_offset", LongType()),
    StructField("symbol", StringType()),
    StructField("trade_ts_kst", TimestampType()),
    StructField("price", DecimalType(18, 2)),
    StructField("volume", LongType()),
    StructField("cum_volume", LongType()),
    StructField("cum_amount", LongType()),
    StructField("trade_side", StringType()),
    StructField("best_ask_price", DecimalType(18, 2)),
    StructField("best_bid_price", DecimalType(18, 2)),
    StructField("raw_payload", StringType()),
])


def _load_fixture(spark, path: Path):
    raw = json.loads(path.read_text())
    rows = []
    for r in raw:
        rows.append((
            datetime.fromisoformat(r["ingest_ts"]),
            r["kafka_partition"], r["kafka_offset"], r["symbol"],
            datetime.fromisoformat(r["trade_ts_kst"]),
            Decimal(r["price"]), r["volume"], r["cum_volume"], r["cum_amount"],
            r["trade_side"], Decimal(r["best_ask_price"]), Decimal(r["best_bid_price"]),
            r["raw_payload"],
        ))
    return spark.createDataFrame(rows, schema=_BRONZE_SCHEMA)


@pytest.fixture
def silver_table(spark):
    spark.sql("CREATE NAMESPACE IF NOT EXISTS local.silver")
    spark.sql("DROP TABLE IF EXISTS local.silver.kis_tick_clean")
    spark.sql("""
      CREATE TABLE local.silver.kis_tick_clean (
        trade_uid string, symbol string,
        trade_ts_kst timestamp, trade_ts_utc timestamp,
        price decimal(18,2), volume bigint, trade_amount decimal(20,2),
        trade_side string, best_ask_price decimal(18,2), best_bid_price decimal(18,2),
        ingest_ts timestamp, silver_ts timestamp
      ) USING iceberg
      PARTITIONED BY (days(trade_ts_kst), hours(trade_ts_kst))
      TBLPROPERTIES ('format-version'='2')
    """)
    yield "local.silver.kis_tick_clean"


def test_e2e_replay_dedup_then_vwap(spark, silver_table, tmp_path):
    fixture = Path(__file__).parent / "fixtures" / "bronze_sample.json"
    bronze = _load_fixture(spark, fixture)

    merge_bronze_into_silver(spark, bronze_df=bronze, silver_table=silver_table)
    silver_count = spark.sql(f"SELECT count(*) c FROM {silver_table}").collect()[0].c
    assert silver_count == 2  # offset 1 and 3 same trade_uid → dedup

    silver_df = spark.table(silver_table)
    gold = compute_vwap_for_hour(spark, silver_df=silver_df,
                                 hour_kst=datetime(2026, 5, 8, 9, 0, 0))
    rows = {(r.symbol, r.ts_minute.minute): r for r in gold.collect()}
    g = rows[("005930", 30)]
    expected_vwap = (Decimal("72500") * 100 + Decimal("72600") * 200) / 300
    assert abs(g.vwap - expected_vwap) < Decimal("0.01")
    assert g.total_volume == 300
    assert g.trade_count == 2
```

- [ ] **Step 3: Run + Commit**

```bash
pytest tests/test_e2e_replay.py -v
git add tests/test_e2e_replay.py tests/fixtures/bronze_sample.json
git commit -m "test(e2e): replay Bronze fixture → Silver dedup → Gold VWAP"
```

---

### Task 39: 100x design doc 정리 (5/14 PM)

Spec §8 — 발표 슬라이드 5번의 backing 문서. dimension 4 분해 + worst-case 비용 + evolution 경로 narrative.

**Files:**
- Create: `docs/superpowers/100x-design.md`

- [ ] **Step 1: 문서 작성 — `docs/superpowers/100x-design.md`**

````markdown
# 100x scale 설계 문서

기준: Phase 1 = 일 100만 trades / 3 종목. 100x = 일 1억 trades / 전 종목 (KOSPI+KOSDAQ ~2,500). spec `docs/superpowers/specs/2026-05-07-tickberg-phase1-mvp-design.md` §8 참조.

## 1. Capacity 스냅샷 (spec §3.7)

| 컴포넌트 | Phase 1 활용률 | 100x 활용률 | 첫 한계 |
|---|---|---|---|
| KIS Producer (Python) | 6% | 100%+ → 분할 | partition 분할 |
| Kafka 단일 broker (KRaft RF=1) | 0.6% | 60% → MSK | MSK Serverless + RF=3 + ISR=2 |
| Kafka partition (key=symbol hot) | 1% | **100%+** | composite key (symbol \|\| minute_bucket) |
| Spark Streaming 단일 worker | 5–36% | **3,600%** | EMR Serverless executor scaling |
| Bronze→Silver MERGE 5min batch | 15–30% | 1,500%+ | partition 병렬 + EMR Serverless |
| Silver→Gold OVERWRITE | <1% | 100% 미만 | 여전히 여유 |

## 2. Dimension 4 분해 (spec §8.3)

### 2.1 Throughput
- KIS WebSocket 1 conn/appkey ~40 종목 → 멀티 appkey 5채널 (200 종목) → 법인계정 (전 종목)
- Spark Streaming 단일 worker → EMR Serverless streaming application + executor auto-scaling
- Kafka single broker RF=1 → MSK Serverless + RF=3 + ISR=2

### 2.2 Batch Window
- Bronze→Silver MERGE 5분 안 못 끝남 → (a) 종목별 partition 병렬, (b) trigger 5→2분, (c) Spark Streaming foreachBatch 분리
- Silver→Gold OVERWRITE → Gold cascade (1초→1분→5분→1시간 sub-aggregations)

### 2.3 Storage / Compaction (장 마감 후)
- Compaction 야간 window (3h) 초과 → day partition 분할 + RewriteManifests + write target file size↑
- expire_snapshots 못 따라잡음 → 일배치 + retention 30→14일
- S3 storage worst case 1.5 TB/월 → Bronze→Glacier IR (90→30일) 적극

### 2.4 Concurrency
- Athena 5GB workgroup → Gold 사전 집계 강화 + tier-2 workgroup + result reuse
- QuickSight SPICE 10GB → Enterprise + 데이터셋 분할
- Glue Catalog 100만 req/월 → 유료 ($1/M)

## 3. 비용 worst case (spec §6.7)

### Phase 1 worst case (월)
| 항목 | 비용 |
|---|---|
| S3 storage | $0.25 |
| S3 PUT/GET | $0.20 |
| Athena scan | $0.03 |
| Glue Catalog | $0 |
| QuickSight Standard Author × 1 | $24 |
| Cross-AZ data transfer | $0.50 |
| **합계** | **~$25–27/월** |

→ AWS Budgets $20 alarm 살짝 초과 → "alarm 동작 검증" narrative.

### 100x worst case (월)
| 항목 | 비용 |
|---|---|
| EMR Serverless 컴퓨트 | $1,300–2,700 |
| MSK Serverless | $50–100 |
| S3 (1.5 TB) + PUT | $145 |
| QuickSight Enterprise (5 user mix) | $100–160 |
| Athena scan (1 TB/월) | $5 |
| Glue Catalog 유료 | $2 |
| **합계** | **$1,600–3,100/월** |

→ Phase 1 budget $20 의 80–155x 초과. **깨지는 곳 = storage 가 아니라 컴퓨트 + QuickSight license**.

## 4. Evolution 경로 (spec §8.5)

```
Phase 1 (1x  =  100만/일):  로컬 Docker · 3 종목 (삼성전자/SK하이닉스/NAVER)
                            Iceberg partition: (days, hour)
       ↓
Phase 2 (10x = 1천만/일):   KOSPI 시총 30 · MSK Serverless · Spark Streaming local 유지
       ↓
Phase 3 (50x = 5천만/일):   KOSPI200 · EMR Serverless streaming · Iceberg branch
                            partition: (bucket(8, symbol), days, hour)
       ↓
Phase 4 (100x = 1억/일):    전 종목 (KOSPI+KOSDAQ) · 멀티 region
                            partition: (bucket(32, symbol), days, hour)
```

각 단계에서 깨지는 컴포넌트 = 다음 단계 진화 트리거 → "한 번에 100x 가지 않음" narrative.

## 5. 발표 메시지 5줄 요약

1. Phase 1 부하 = 평균 45/sec / 피크 300/sec — 단일 process로도 처리 가능
2. **stream을 굳이 쓰는 이유** = peak burst 흡수 + 100x 학습 baseline (한계 도달 시점·방향 명확)
3. **첫 깨지는 곳** = Kafka partition (key=symbol → 삼성전자 hot) + Spark Streaming 단일 worker
4. **비용 worst-case** = Phase 1 $25 (budget $20 살짝 초과 = alarm 검증) / 100x $1,600–3,100 (storage 아닌 컴퓨트)
5. 진화 = 1→10→50→100x 4단계, 각 단계 trigger = 깨지는 컴포넌트
````

- [ ] **Step 2: Commit**

```bash
git add docs/superpowers/100x-design.md
git commit -m "docs: 100x scale design (capacity / dimension / cost / evolution)"
```

---

### Task 40: 5/14 영업시간 녹화 + Demo runbook 1B + smoke 강화

Spec §7.2 — 5/14 목 영업시간 녹화 (오전 09:30 또는 오후 14:30, 5–10분).

**Files:**
- Create: `docs/superpowers/demo-runbook-1b.md`
- Modify: `infra/scripts/smoke_check.sh` (DART/credit_info DAG 추가)

- [ ] **Step 1: smoke 강화 — `infra/scripts/smoke_check.sh` 의 `[5] Airflow DAGs` 섹션 dag list 에 추가**

```bash
for dag in bronze_to_silver_kis silver_to_gold_vwap dim_symbol_daily \
           iceberg_compaction expire_snapshots dart_ingest_daily; do
```

- [ ] **Step 2: `docs/superpowers/demo-runbook-1b.md`**

````markdown
# 5/16 최종 발표 runbook

## T-30min smoke
```bash
bash infra/scripts/smoke_check.sh
```
6개 DAG + 모든 컨테이너 + KIS/Bronze/Athena/Grafana ALL CLEAR.

## 발표 흐름 (10–12분, spec §8.6)
1. (1min) 동기·결정 — 한국 주식 lakehouse + AWS 단일 + 메달리온
2. (3min) 시연 — **5/14 녹화 영상** (KIS streaming + Airflow 5min cycle + Grafana 4 패널 + QuickSight) + Athena live (`SELECT * FROM gold_symbol_vwap_1m WHERE ts_minute >= current_timestamp - interval '1' hour`)
3. (2min) **Iceberg 정당화** — MERGE dedup (실 운영 가치) + OVERWRITE 원자성 (대시보드 일관성) + time-travel audit (`SELECT * FROM silver_kis_tick_clean FOR TIMESTAMP AS OF '2026-05-14 12:00:00'`)
4. (2min) **운영 가시성** — 4-tier 구조 + 5분 헬스체크 시나리오 (spec §5.2)
5. (2min) **100x scale** — `docs/superpowers/100x-design.md` 5줄 요약 + dimension 4 + worst-case 비용 + evolution 경로
6. (1min) **Phase 2 로드맵** — dbt Semantic Layer / 자동매매 (Signal+Order+Risk) / MSK Serverless / EMR Serverless / Trino. CLAUDE.md "Out of Scope" 섹션 그대로

## 평가 4가지 ↔ 슬라이드 매핑
- 운영 가시성 → 슬라이드 4
- 100x scale → 슬라이드 5
- Iceberg 필요성 → 슬라이드 3
- 협업·지속가능성 → 슬라이드 6 + CLAUDE.md + design doc + plan + 본 runbook

## 백업
- 5/14 녹화 영상 (오전 + 오후 2개)
- Athena 결과 스크린샷 (각 health-query)
- Grafana snapshot JSON
- QuickSight PDF export
````

- [ ] **Step 3: 5/14 목 영업시간에 실제 녹화 (manual)**

5/14 09:30 또는 14:30 KST. 녹화 화면:
- (A) Grafana 1A + 1B 두 dashboard 순차
- (B) Airflow UI — 6 DAG 모두 unpaused + 최근 run success
- (C) QuickSight 운영탭 (6 viz)
- (D) Athena live — `04_gold_partition_completeness.sql`

녹화 산출물 → `docs/superpowers/recordings/2026-05-14-1b.mp4` (커밋 X).

- [ ] **Step 4: Commit**

```bash
git add docs/superpowers/demo-runbook-1b.md infra/scripts/smoke_check.sh
git commit -m "docs(demo): 1B runbook + smoke check covers 6 DAGs"
```

---

### Task 41: PPT 슬라이드 자료 정리 + Phase 2 roadmap doc + README 정리

5/14 목 PM (PPT 자정 마감 전 마지막 정리). PPT 자체 작성은 사용자 (이 task는 자료 정리).

**Files:**
- Create: `docs/superpowers/phase2-roadmap.md`
- Create: `docs/superpowers/ppt-source-material.md`
- Modify: `README.md` (Phase 1 완성 상태 반영)

- [ ] **Step 1: Phase 2 roadmap — `docs/superpowers/phase2-roadmap.md`**

CLAUDE.md "Out of Scope" 항목들을 Phase 2 작업 묶음으로 expand. spec §9 참조.

````markdown
# Phase 2 Roadmap

| Theme | 항목 | trigger | 예상 작업량 |
|---|---|---|---|
| Semantic Layer | dbt 도입, dim 1차 모델링 | 데이터 소스 5+ | 2–3주 |
| 자동매매 | Signal Generator + Order Executor + Risk Manager (`code/pipelines/trading/`), `TRADING_ENABLED=true` | 모의계좌 backtesting 1주 | 4–6주 |
| 데이터 소스 확장 | Alpaca / 미국 / 암호화폐 추가, 종목 universe 30→200→KOSPI200 | Phase 2 자동매매 검증 후 | 2–3주 |
| 인프라 진화 | EMR Serverless streaming + MSK Serverless RF=3 + ISR=2 | Phase 1 부하 6×, 100x baseline | 1–2주 |
| 데이터 품질 | Great Expectations 또는 dbt test (Phase 1 health-queries 가 baseline) | dim 모델 SCD2 도입 | 1주 |
| 보안 | AWS Secrets Manager 마이그레이션 (현재 .env) | 운영 전환 결정 | 3일 |
| 카탈로그 시각화 | Trino 도입 (필요 시) | Athena 한계 도달 | 1주 |
| Replay tool | KIS WebSocket replay (현재 = 영업시간 녹화로 대체) | 비영업일 demo 자주 필요 시 | 1주 |
| Audit | Iceberg snapshot 분기별 archive DAG (현재 retention 30일) | 회계감사·법무 요구 | 3일 |
````

- [ ] **Step 2: PPT source — `docs/superpowers/ppt-source-material.md`**

````markdown
# 5/14 PPT 자료 (사용자가 슬라이드 작성 시 인용할 source)

## 슬라이드 1 (동기·결정)
- 메타코드 DE 부트캠프 8회차 최종 프로젝트 (CLAUDE.md)
- 한국 주식 실시간 체결가 → S3 Iceberg Lakehouse → Athena/QuickSight
- 핵심 결정 4: AWS 단일 환경 / Bronze=Parquet, Silver/Gold=Iceberg / dbt·자동매매 Phase 2 / 종목 3 (삼성전자·SK하이닉스·NAVER)

## 슬라이드 2 (시스템 시연)
- 동영상 임베드: `docs/superpowers/recordings/2026-05-14-1b.mp4`
- Athena live query: `code/health-queries/04_gold_partition_completeness.sql`

## 슬라이드 3 (Iceberg 3 가치)
spec §3.5 표 그대로:
- ① MERGE INTO → silver.kis_tick_clean dedup (streaming 재시작 시 Bronze 중복 자동 흡수)
- ② OVERWRITE 원자성 → gold.symbol_vwap_1m hour partition (대시보드 일관성)
- ③ Time-travel → 어제 12:00 silver snapshot (audit)

"Parquet+Glue 안 되나?" 답: atomic upsert 불가 / mid-write race / snapshot 없음.

## 슬라이드 4 (운영 가시성)
- 4-tier (T1 Grafana / T2 Airflow / T3 QuickSight / T4 Athena ad-hoc)
- 5분 헬스체크 시나리오 (spec §5.2 그대로)
- 장 시간대 vs 장 마감 후 활성 모니터링 분리 (spec §4.4)

## 슬라이드 5 (100x scale)
- `docs/superpowers/100x-design.md` 5줄 요약
- Dimension 4 표 + worst-case 비용 + evolution 4단계

## 슬라이드 6 (협업·지속가능성 + Phase 2)
- 자산: CLAUDE.md / spec doc / plan doc / runbook (1A + 1B) / health-queries / 100x-design / phase2-roadmap
- 6개월 후 새 팀원 합류 가능성 — 모든 결정 D1–D21 표 (spec Decision Log) 로 추적 가능
- Phase 2 = `docs/superpowers/phase2-roadmap.md`

## 부록 (Q&A 대비)
- 비용: Phase 1 $25/월 (alarm 동작 검증) / 100x $1,600–3,100/월 (컴퓨트 dominant)
- Kafka RF=1 → 운영 전환 시 RF=3 + ISR=2 + MSK
- Spark Streaming local → EMR Serverless executor scaling
- partition key=symbol hot → bucket(N, symbol) hash
- 발표일=비영업일 → 녹화 영상 (5/14 영업시간) + Athena live
````

- [ ] **Step 3: README 마무리 — `README.md` 전면 보강**

```markdown
# tickberg — Real-time Korean Stock Market Tick Lakehouse

메타코드 DE 부트캠프 8회차 최종 프로젝트 (Public 포트폴리오).

## 컨셉
한국투자증권 실시간 체결가 + DART 공시 + 신용정보원 → Bronze/Silver/Gold 메달리온 → S3 Iceberg Lakehouse → Athena → QuickSight.

## 아키텍처
- AWS 단일 환경 (S3 + Glue + Athena v3 + QuickSight, ap-northeast-2)
- 컴퓨트만 로컬 Docker (Kafka KRaft, Spark Standalone, Airflow LocalExecutor, Prometheus, Grafana)
- Bronze = Parquet (streaming append-only). Silver/Gold = Iceberg v2 (MERGE / OVERWRITE / time-travel)

## 핵심 산출물
- 설계 문서: `docs/superpowers/specs/2026-05-07-tickberg-phase1-mvp-design.md`
- 구현 plan: `docs/superpowers/plans/2026-05-07-tickberg-phase1-mvp.md`
- 100x scale 설계: `docs/superpowers/100x-design.md`
- Phase 2 로드맵: `docs/superpowers/phase2-roadmap.md`
- Demo runbook: `docs/superpowers/demo-runbook-1a.md`, `docs/superpowers/demo-runbook-1b.md`

## Quick Start
1. `cp .env.example .env` (KIS / DART / AWS profile 채움)
2. `bash infra/scripts/aws_initial_setup.sh`
3. `bash infra/scripts/run_ddl.sh`
4. `docker compose -f infra/docker/docker-compose.yml up -d`
5. `bash infra/scripts/kafka_create_topics.sh`
6. Airflow http://localhost:8080 (admin/admin) → 6 DAG unpause
7. Grafana http://localhost:3000 → "tickberg — 1차 운영 패널" + "1B 풍부화 패널"
8. Athena workgroup `tickberg-wg` 에서 `code/health-queries/*.sql` 실행

## Tests
```bash
pytest tests/   # unit + integration + E2E replay
```

## Demo
- 1차 (5/10 일): `docs/superpowers/demo-runbook-1a.md`
- 최종 (5/16 토): `docs/superpowers/demo-runbook-1b.md`
- 30분 전 smoke: `bash infra/scripts/smoke_check.sh`

## 비용
Phase 1 worst case ~$25–27/월. AWS Budgets $20 alarm 동작 검증.

## License
MIT.
```

- [ ] **Step 4: Commit**

```bash
git add docs/superpowers/phase2-roadmap.md docs/superpowers/ppt-source-material.md README.md
git commit -m "docs: phase2 roadmap + PPT source material + README finalize"
```

---

### Milestone 1B 완료 게이트 (5/13 수 21:00 cutoff = 모든 fallback 결정 종료)

이 시점에 다음이 모두 완료되어야 함 (spec §7.5):

- [ ] DART 일배치 적재 + Silver MERGE 동작 (또는 fallback: Bronze + Athena 직접 쿼리)
- [ ] 신용정보원 Bronze 적재 1회 검증 (또는 cut)
- [ ] `expire_snapshots` DAG 활성 + 첫 자동 실행 검증 (5/13 수 까지)
- [ ] Grafana 1B dashboard 4 패널 + 새 alert rule 2개
- [ ] Health queries 5/6/7 모두 SUCCEEDED
- [ ] QuickSight 운영탭 6 viz 셋업 (3 + 3)
- [ ] `pytest -q tests/` 모두 PASS (streaming integration + e2e replay 포함, 부분 cut 시 skip 표시)
- [ ] `bash infra/scripts/smoke_check.sh` ALL CLEAR (영업시간이라면)

**5/13 수 21:00 = 모든 fallback 결정 종료** → 5/14 목 = 마지막 영업일 녹화 (오전·오후) + 100x doc 완료 + PPT 자료 정리 + **24:00 PPT 제출**.

**5/15 금 = α** (PPT freeze 후 보너스): 추가 개발·다듬기·5/16 발표 리허설. PPT 변경 X.

**5/16 토 = 발표** — `bash infra/scripts/smoke_check.sh` → 리허설 → 발표.

---

## Self-Review (writing-plans skill 요구)

### 1. Spec 커버리지 — 각 spec 섹션이 어떤 task 에서 처리되는지

| Spec 섹션 | Task |
|---|---|
| §2.1 컴포넌트 (KIS Producer/Spark/Airflow/Kafka/Prom/Grafana/AWS) | T2 (AWS), T3-4 (compose), T8-14 (KIS Producer), T15 (Spark image), T21 (Airflow base), T26 (Grafana) |
| §2.2 데이터 흐름 | T16 (streaming Bronze), T17 (Silver MERGE), T18 (Gold), T19 (dim_symbol) |
| §2.3 토폴로지 (FairScheduler) | T16 (fairscheduler.xml) |
| §2.4 Kafka topic & partition (12, key=symbol, RF=1) | T4 (`kafka_create_topics.sh`) |
| §3.1 Bronze DDL | T5 |
| §3.2 Silver kis_tick_clean DDL | T6 |
| §3.3 Silver dim_symbol DDL | T6 |
| §3.4 Gold DDL | T7 |
| §3.5 Iceberg 3 가치 | T17 (MERGE), T18 (OVERWRITE), runbook (time-travel demo) |
| §3.7 Capacity 검증 | T39 (100x doc) |
| §4.1 1min trigger | T16 |
| §4.2 Airflow DAG 5개 | T22-24 (1A: 4개) + T33 (1B: expire_snapshots) + T30 (DART) |
| §4.3 Idempotency 4단계 | T17/18/19 구현 + T16 checkpoint |
| §4.4 시간대 분리 | DAG cron + smoke check 분기 |
| §4.5 코드 배치 | T8-15, T17-20, T22-24, T30-33 |
| §5.1 4-tier | T22 (T2 Airflow), T26 (T1 Grafana), T27 (T3 QuickSight), T25 (T4 Athena) |
| §5.2 5분 헬스체크 | runbook 1A/1B + smoke_check.sh |
| §5.3 Grafana 패널 (1차 2개 + 1B 4개) | T26, T34 |
| §5.4 QuickSight 운영탭 (1차 3 + 1B 3) | T27, T36 |
| §5.5 Health queries (1차 4 + 1B 3) | T25, T35 |
| §6.1 KIS Producer 안정성 + 6.1.1 token refresh 03:30 | T10 (reconnect), T12 (refresher) |
| §6.2 Kafka producer 설정 + RF 정책 | T13 (main.py producer config) |
| §6.3 Spark checkpoint | T16 |
| §6.4 Iceberg commit 충돌 방지 | T22-24 (max_active_runs=1) + 시간 분리 |
| §6.5 Airflow 정책 (retries, sla) | T22-24 default_args |
| §6.6 비용 가드레일 | T2 (S3 lifecycle, Athena 5GB cutoff, Budgets) |
| §6.7 비용 추정 worst case | T39 (100x doc) |
| §6.8 Secret management | T1 (.env.example) |
| §6.9 30분 전 sanity | T29 (smoke_check.sh) |
| §7.1 비영업일 demo strategy | T28 (5/8 녹화), T40 (5/14 녹화) |
| §7.3 Test 자산 (1차) | T8 (parser unit), T17 (Silver MERGE), T25 (health), T29 (smoke) |
| §7.3.1 Test 강화 (1B) | T37 (streaming integration), T38 (E2E replay) |
| §7.4 5/10 Risk → Fallback | runbook 1A + Milestone 1A 완료 게이트 |
| §7.5 5/16 복구 plan | Milestone 1B 완료 게이트 |
| §8.1 6일 캘린더 | Milestone 1A/1B 일정 mapping |
| §8.3 100x dimension 4 | T39 |
| §8.5 Evolution 경로 | T39 |
| §8.6 발표 구조 | runbook 1B |
| §9 Phase 2 deferred | T29 (`code/pipelines/trading/.gitkeep`), T41 (`phase2-roadmap.md`) |

**Gap**: spec D20 (Iceberg ① MERGE 시연 위치 = silver.kis_tick_clean dedup) → T17 통합 테스트 + runbook 의 demo SQL 로 cover. dim_symbol 가상 액면분할 시연 X (의도된 결정).

### 2. Placeholder scan
- "TBD", "TODO", "implement later" — 없음
- "Add appropriate error handling" — 없음 (구체 try/except 명시)
- "Similar to Task N" — 없음 (각 task 코드 독립 작성)
- "Write tests for the above" 없는 placeholder — 모든 test step 에 실제 코드

### 3. Type / signature consistency
- `merge_bronze_into_silver(spark, *, bronze_df, silver_table)` — T17 정의, T38 사용. 일치.
- `compute_vwap_for_hour(spark, *, silver_df, hour_kst)` — T18 정의, T38 사용. 일치.
- `merge_dim_symbol(spark, *, src_df, dim_table)` — T19. 일치.
- `compact_table(spark, *, table, target_file_size_bytes, ...)` — T20.
- `KisAuth.refresh()` — T9 정의, T13 main.py + T12 refresher 가 호출. 일치.
- `KafkaTickPublisher.publish(row)` — T11 정의, T13 main.py 사용. 일치.
- `KisWebSocket.stream()` — T10 정의, T13 main.py 사용. 일치.
- `_common.spark_submit_command(script, *args)` — T21 정의, T22/23/24/30/33 사용. 일치.
- `DartClient.fetch_disclosures(business_date, stock_codes)` — T30 정의, ingest 사용. 일치.

### 4. 알려진 위험 (별도 task 없이 runbook/smoke 로 대응)
- Airflow image docker.sock group permission (mac vs linux) — T21 step 5 sanity check
- KIS WebSocket IP 등록 미리 (사용자 manual 1주 전) — runbook 1A T-30min 외부 사항
- QuickSight Author license 비용 — T27/T36 runbook + 비용 worst-case T39 에 명시
- testcontainers Kafka 가 macOS Apple Silicon 에서 느림 — T37 cut 우선순위 D2 (PPT 마감 후 5/15 α)

---

## Execution Handoff

Plan complete and saved to `docs/superpowers/plans/2026-05-07-tickberg-phase1-mvp.md`. Two execution options:

**1. Subagent-Driven (recommended)** — fresh subagent per task + two-stage review. 41 task가 많아 main session context 보호에 유리. Iceberg/Spark 첫 사용 task (T15-17) 는 subagent 가 시행착오 흡수.

**2. Inline Execution** — batch execution with checkpoints. 빠르지만 main session context 부담. 5/8 영업시간 녹화 prep / 사용자 manual 셋업 (QuickSight, AWS 콘솔) 단계마다 자연스러운 checkpoint.

Subagent 방식 시: REQUIRED SUB-SKILL `superpowers:subagent-driven-development`.  
Inline 방식 시: REQUIRED SUB-SKILL `superpowers:executing-plans`.

**어떤 방식으로 진행할까요?**









