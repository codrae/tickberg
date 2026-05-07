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
