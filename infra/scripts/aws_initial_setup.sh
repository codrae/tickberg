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
