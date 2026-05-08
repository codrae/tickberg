#!/usr/bin/env bash
set -euo pipefail
: "${AWS_PROFILE:=default}"
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
