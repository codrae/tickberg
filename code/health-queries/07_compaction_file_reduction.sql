-- Compaction 효과 검증 — 일배치 18:00 KST 후 평균 file size 가 target (256MB) 근처여야 함.
-- write 시 256MB target 이라도 1분 trigger streaming → small files (수십 MB) 누적
-- → Compaction 이 hour 파티션 별로 병합. 결과 file 수 ↓, 평균 size ↑.
SELECT
  count(*)                                      AS file_count,
  round(sum(file_size_in_bytes) / 1048576.0, 2) AS total_mb,
  round(avg(file_size_in_bytes) / 1048576.0, 2) AS avg_mb,
  round(min(file_size_in_bytes) / 1048576.0, 2) AS min_mb,
  round(max(file_size_in_bytes) / 1048576.0, 2) AS max_mb,
  CASE
    WHEN avg(file_size_in_bytes) >= 256 * 1048576 THEN 'OK (>=256MB target 도달)'
    WHEN avg(file_size_in_bytes) >=  64 * 1048576 THEN 'WARN (Compaction 후 정상 범위)'
    ELSE                                                'BAD (small-files 누적 — Compaction 미실행 의심)'
  END AS verdict
FROM "tickberg"."silver_kis_tick_clean$files";
