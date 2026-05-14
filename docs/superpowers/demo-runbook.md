# tickberg 발표 runbook — 5/16 토 최종

> 5/10 1차 발표 종료. 본 runbook 은 **5/16 최종 발표 (10–12분)** + **5/14 24:00 PPT 제출** 기준.
> 발표 30분 전 `bash infra/scripts/smoke_check.sh` 가 ALL CLEAR 면 진행.

## T-30min smoke

```bash
bash infra/scripts/smoke_check.sh
```

8 단계 (컨테이너 / Kafka / KIS WS / Bronze / DAG / Athena / Grafana / Prometheus) 모두 OK 면 ALL CLEAR.
1개라도 FAIL → spec `docs/superpowers/specs/2026-05-07-tickberg-phase1-mvp-design.md` §7.4 fallback 매트릭스 참조.

**비영업일 (5/16 토)** = `[3] kis_ws_connected=0` 과 `[4] no bronze for today` 는 알려진 WARN. 그 외 모두 OK 면 진행.

## 발표 흐름 (10–12분, slide 33장)

| # | 시간 | 내용 | 자료 |
|---|---|---|---|
| 1 | 1min | 동기·결정 — 한국 주식 lakehouse · AWS 단일 환경 · 메달리온 3계층 | slide 1–3 |
| 2 | 3min | 시연 — **5/14 영업시간 녹화 영상** + Athena live SELECT | recordings/ + 미리 입력된 SQL |
| 3 | 2min | Iceberg ① MERGE dedup + ② OVERWRITE 원자성 + ③ time-travel | `tests/test_silver_merge.py` + Athena `$snapshots` |
| 4 | 2min | 운영 가시성 — Grafana 패널 + Airflow UI 5분 cycle + 5분 헬스체크 | Grafana + Airflow UI |
| 5 | 2min | 100x scale 사고 — Dimension 4 + 비용 worst/best + Evolution 경로 | `docs/architecture/100x-scale.md` |
| 6 | 1–2min | Phase 2 로드맵 — dbt · 자동매매 · MSK · EMR Serverless | slide 마지막 |

## 발표 시 라이브 자료

### Athena 쿼리 (미리 입력해둘 것)

```sql
-- 1. Iceberg MERGE 효과 — silver_kis_tick_clean 의 snapshot history
SELECT committed_at, operation, summary
FROM "tickberg"."silver_kis_tick_clean$snapshots"
ORDER BY committed_at DESC LIMIT 5;

-- 2. dim_symbol JOIN — 종목명 매핑 (KOSPI 액면분할 시연 narrative)
SELECT g.symbol, d.symbol_name, g.ts_minute, g.vwap, g.total_volume
FROM tickberg.gold_symbol_vwap_1m g
LEFT JOIN tickberg.silver_dim_symbol d ON g.symbol = d.symbol
WHERE g.ts_minute >= current_timestamp - INTERVAL '3' HOUR
ORDER BY g.ts_minute DESC LIMIT 20;

-- 3. DART 공시 timeline — Phase 1B 추가
SELECT d.rcept_date, s.symbol_name, d.report_nm, d.report_kind, d.flr_nm
FROM tickberg.silver_dart_disclosure_clean d
LEFT JOIN tickberg.silver_dim_symbol s ON d.stock_code = s.symbol
ORDER BY d.rcept_date DESC LIMIT 10;
```

### Grafana 화면 공유 URL

- 1차 대시보드: http://localhost:3000/d/tickberg/tickberg
- 1B 풍부화 대시보드 (Task 34): http://localhost:3000/d/tickberg-1b/tickberg-1b

### Superset 대시보드

- 발표 직전 `docker compose -f infra/docker/docker-compose.yml up -d superset`
- http://localhost:8088 admin/admin
- KPI 탭 + 운영 탭 화면 공유

## 백업 (라이브 끊김 시)

1. **AWS / Athena 다운** → `docs/superpowers/recordings/screenshots/` 스크린샷 (4장)
2. **Grafana 다운** → JSON snapshot export (Grafana → Share → Snapshot)
3. **WebSocket 끊김** → 영업시간 녹화 영상 재생, 라이브 시도 X

## T-0 발표 직전 (10분 전)

- [ ] KIS API 콘솔에서 발표용 IP 등록 확인 (WebSocket 차단 방지 — 1주일 전 한번)
- [ ] AWS 콘솔 + Athena 탭 1개 미리 로그인 + 쿼리 입력
- [ ] Grafana + Superset + Airflow UI 탭 3개 미리 열기
- [ ] 화면 공유 + 마이크 테스트
- [ ] PPT 발표자 모드 (제출 PPT = freeze, 다른 보강 자료 별도)

## T-0 + 발표 후

- [ ] 평가위원 질의응답 대응 자료:
  - "왜 EMR/MSK 안 썼나" → 100x doc §7 evolution 경로
  - "왜 Phase 1 baseline 이 streaming 정당화 약한가" → spec §3.7 narrative
  - "Iceberg 가치 한 줄로" → ① MERGE ② OVERWRITE ③ time-travel
  - "Phase 2 timeline" → 100x doc §11 open questions

## 평가 4기준 매핑

| 기준 | 자료 (commit / 파일) |
|---|---|
| 운영 가시성 | Grafana `348d5c1` + health-queries 1–7 + smoke_check.sh |
| 100x 스케일 | `docs/architecture/100x-scale.md` `4bb1aa1` |
| Iceberg 필요성 | Silver MERGE `bb5b224` / `d9bacac` + Gold OVERWRITE `1ded4c1` + test `f196954` |
| 협업·지속가능성 | spec + plan + 100x doc + CLAUDE.md + Superset md + DAG 4종 + smoke 자동화 |
