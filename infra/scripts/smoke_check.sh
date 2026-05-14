#!/usr/bin/env bash
# 발표 30분 전 헬스체크 — 5분 안에 모든 항목 PASS 되어야 시연 안전.
# 영업시간 외라면 [3] kis_ws_connected · [4] today bronze partition 은
# 알고 있는 FAIL (정상) — 그 외 항목 모두 PASS 면 시연 진행 가능.
set -euo pipefail

: "${AWS_PROFILE:=default}"
: "${AWS_REGION:=ap-northeast-2}"
: "${S3_BUCKET:=tickberg-lakehouse}"

ok()   { printf "  \033[32mOK\033[0m    %s\n" "$1"; }
warn() { printf "  \033[33mWARN\033[0m  %s\n" "$1"; }
err()  { printf "  \033[31mFAIL\033[0m  %s\n" "$1"; FAILED=$((FAILED+1)); }
FAILED=0

echo "[1] containers up"
for svc in tickberg-kafka tickberg-spark-master tickberg-spark-worker \
           tickberg-spark-streaming tickberg-airflow-scheduler \
           tickberg-airflow-web tickberg-prometheus tickberg-grafana \
           tickberg-kis-producer; do
  state=$(docker inspect -f '{{.State.Running}}' "$svc" 2>/dev/null || echo "missing")
  case "$state" in
    true)    ok "$svc";;
    false)   err "$svc stopped";;
    missing) err "$svc not deployed";;
  esac
done

echo
echo "[2] Kafka topic kis.tick.raw"
# KAFKA_JMX_OPTS="" — JMX exporter 가 9999 점유 중이므로 CLI 자체 JMX agent 비활성화
docker exec -e KAFKA_JMX_OPTS="" tickberg-kafka kafka-topics.sh \
  --bootstrap-server localhost:9092 --describe --topic kis.tick.raw >/dev/null 2>&1 \
  && ok "topic kis.tick.raw exists" || err "topic missing"

echo
echo "[3] KIS producer health metrics (영업시간만 ws_connected=1 기대)"
WS=$(curl -sf localhost:9100/metrics 2>/dev/null | grep '^kis_ws_connected ' | awk '{print $2}' || echo "?")
case "$WS" in
  "1"|"1.0") ok "kis_ws_connected=1";;
  "0"|"0.0") warn "kis_ws_connected=0 (장 마감 후엔 정상)";;
  *)         err "kis_ws_connected 메트릭 조회 실패";;
esac

echo
echo "[4] Bronze partition for today (영업시간만 기대)"
TODAY=$(date +%Y-%m-%d)
N=$(aws --profile "$AWS_PROFILE" --region "$AWS_REGION" s3 ls \
    "s3://${S3_BUCKET}/bronze/kis_tick_raw/dt=${TODAY}/" 2>/dev/null | wc -l)
if [ "$N" -gt 0 ]; then
  ok "bronze partition dt=${TODAY} ($N entries)"
else
  YESTERDAY=$(date -v-1d +%Y-%m-%d 2>/dev/null || date -d "yesterday" +%Y-%m-%d)
  N2=$(aws --profile "$AWS_PROFILE" --region "$AWS_REGION" s3 ls \
       "s3://${S3_BUCKET}/bronze/kis_tick_raw/dt=${YESTERDAY}/" 2>/dev/null | wc -l)
  if [ "$N2" -gt 0 ]; then
    warn "no bronze for today — 이전 영업일 dt=${YESTERDAY} ($N2 entries) 존재 (장 마감 후 정상)"
  else
    err "bronze partition 없음 (오늘·전일 모두)"
  fi
fi

echo
echo "[5] Airflow DAGs unpaused"
for dag in bronze_to_silver_kis silver_to_gold_vwap dim_symbol_daily \
           iceberg_compaction expire_snapshots dart_ingest_daily; do
  state=$(docker exec tickberg-airflow-scheduler airflow dags details "$dag" \
            2>/dev/null | grep -E '^is_paused\b' | awk '{print $3}' || echo "?")
  case "$state" in
    False) ok "$dag unpaused";;
    True)  err "$dag paused";;
    *)     err "$dag missing or scheduler error";;
  esac
done

echo
echo "[6] Athena health-query smoke (scan ≤ 5GB)"
bash infra/scripts/run_health_query.sh code/health-queries/03_symbol_coverage.sql \
  >/dev/null 2>&1 && ok "symbol_coverage query OK" || err "athena query failed"

echo
echo "[7] Grafana dashboard reachable"
curl -sf http://localhost:3000/api/health >/dev/null \
  && ok "grafana healthy" || err "grafana down"

echo
echo "[8] Prometheus targets all UP"
DOWN=$(curl -sf 'http://localhost:9090/api/v1/targets?state=active' 2>/dev/null \
       | grep -o '"health":"down"' | wc -l)
if [ "$DOWN" -eq 0 ]; then
  ok "all prometheus targets UP"
else
  err "$DOWN prometheus target(s) down"
fi

echo
if [ "$FAILED" -eq 0 ]; then
  printf "\033[32mALL CLEAR\033[0m — demo 안전\n"
  exit 0
else
  printf "\033[31m%d 실패\033[0m — fallback 결정 필요 (spec §7.4)\n" "$FAILED"
  exit 1
fi
