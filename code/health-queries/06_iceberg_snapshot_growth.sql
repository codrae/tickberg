-- Iceberg snapshot 누적 모니터링 — 주 1회 expire_snapshots (Sun 19:00 KST) 가 30일
-- 이전 snapshot 정리. snapshot_count 가 1,000+ 면 expire DAG 실패 의심.
-- 매주 snapshot growth rate ≈ (영업일 5 × 12h × 60 trigger) = ~3,600/주 추정.
SELECT
  'silver_kis_tick_clean' AS table_name,
  count(*)                AS snapshot_count,
  min(committed_at)       AS oldest,
  max(committed_at)       AS newest
FROM "tickberg"."silver_kis_tick_clean$snapshots"

UNION ALL

SELECT
  'silver_dart_disclosure_clean',
  count(*), min(committed_at), max(committed_at)
FROM "tickberg"."silver_dart_disclosure_clean$snapshots"

UNION ALL

SELECT
  'gold_symbol_vwap_1m',
  count(*), min(committed_at), max(committed_at)
FROM "tickberg"."gold_symbol_vwap_1m$snapshots"

ORDER BY table_name;
