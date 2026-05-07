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
