# Superset 셋업 — Phase 1 발표용 BI

> Grafana 와 역할 분리:
> - **Grafana** = 시스템 헬스 (Prometheus 메트릭, 프로세스/JVM/Kafka throughput, 5초~1분, 운영자 page)
> - **Superset** = 데이터 품질 + 비즈니스 KPI (Athena SQL, 1~5분, 발표·DQ 분석)
>
> Producer 가 `ws_connected=1` 이어도 Spark Streaming 이 죽으면 Grafana 는 green / Superset 은 stale → 두 lens 둘 다 필수.

## 사전 조건

- `infra/docker/docker-compose.yml` 의 `superset` service 빌드 완료 (`docker compose -f infra/docker/docker-compose.yml build superset`)
- `airflow-postgres` healthy (Superset 메타데이터 DB 를 같은 인스턴스의 `superset` database 에 둠 — `init-and-start.sh` 가 idempotent 생성)
- `~/.aws/credentials` 의 `default` profile 이 Athena workgroup `tickberg-wg` 접근 가능
- Glue 카탈로그에 등록된 테이블: `bronze_kis_tick_raw`, `silver_kis_tick_clean`, `silver_dim_symbol`, `gold_symbol_vwap_1m`, `bronze_dart_disclosure_raw`

## 시작 / 정지

```bash
# 시작 (정책: restart=no — 발표 외 시간엔 정지로 Athena 비용 회피)
docker compose -f infra/docker/docker-compose.yml up -d superset

# 헬스체크 (8088 응답)
curl -sf http://localhost:8088/health && echo OK

# 정지
docker compose -f infra/docker/docker-compose.yml stop superset
```

초기 admin 계정: `admin / admin` (`init-and-start.sh` 가 첫 기동 시 자동 생성, idempotent).
운영 전환 시 변경 필수.

## Athena Database 연결 (UI 최초 1회)

Settings → Database Connections → `+ DATABASE` → Athena (PyAthena driver) → 다음 SQLAlchemy URI:

```
awsathena+rest://:@athena.ap-northeast-2.amazonaws.com:443/tickberg?s3_staging_dir=s3%3A%2F%2Ftickberg-lakehouse%2Fathena-results%2F&work_group=tickberg-wg
```

- 자격증명은 컨테이너의 `AWS_PROFILE=default` + 마운트된 `~/.aws/credentials` 로 해결됨 → URI 에 access key 직접 X
- 연결 테스트 → `tickberg` DB 의 5 테이블이 보이면 OK
- Display name: `tickberg-athena`

## Dataset 등록

Datasets → `+ DATASET` → Database = `tickberg-athena`, Schema = `tickberg`. 다음 5 테이블 등록:

| Dataset | 출처 | 용도 |
|---|---|---|
| `bronze_kis_tick_raw` | Parquet | freshness KPI |
| `silver_kis_tick_clean` | Iceberg | coverage, throughput, late arrival |
| `silver_dim_symbol` | Iceberg | symbol → corp_name 매핑 (JOIN 용) |
| `gold_symbol_vwap_1m` | Iceberg | 분봉 VWAP, 거래량, OHLC |
| `bronze_dart_disclosure_raw` | Parquet | 종목별 공시 타임라인 |

## 대시보드 구성 (7 chart, 2 탭)

### KPI 탭 — 비즈니스 (3 chart)

1. **VWAP 추세 (Line)**
   - dataset: `gold_symbol_vwap_1m` JOIN `silver_dim_symbol` ON symbol
   - x: `ts_minute`, y: `vwap`, group by: `corp_name`
   - filter: `ts_minute >= TODAY()` + KST 영업시간 (09:00–15:30)

2. **분봉 거래량 (Bar)**
   - dataset: `gold_symbol_vwap_1m`
   - x: `ts_minute`, y: `total_volume`, group by: `symbol`

3. **종목별 거래량 점유율 (Pie)**
   - dataset: `gold_symbol_vwap_1m`
   - dimension: `corp_name` (dim_symbol JOIN), metric: `SUM(total_volume)`
   - 영업일 1일 누적

### 운영 탭 — Data Quality (3 chart) + DART 타임라인 (1 chart)

1. **Bronze Freshness KPI (Big Number)**
   - dataset: `bronze_kis_tick_raw`
   - metric: `DATEDIFF('minute', MAX(ingest_ts), NOW())`
   - subheader: "Bronze 마지막 적재 후 경과 (분)"
   - threshold: 노란 5분, 빨간 10분 (장 시간대 한정 — 장 마감 후엔 자연스럽게 클 수밖에 없음)

2. **Symbol Coverage Bar (최근 1h)**
   - dataset: `silver_kis_tick_clean`
   - x: `symbol` (또는 dim JOIN 후 `corp_name`), y: `COUNT(*)`
   - filter: `silver_ts >= NOW() - INTERVAL '1' HOUR`
   - 3 막대 (삼성/하이닉스/NAVER) — 누락 시 즉시 식별

3. **Silver Throughput per 5min (24h)**
   - dataset: `silver_kis_tick_clean`
   - x: `DATE_TRUNC('5min', silver_ts)`, y: `COUNT(*)`
   - 영업시간 패턴 시각화 → 09:00 급상승, 15:30 종료 vis 검증

4. **DART 공시 타임라인 (Table)**
   - dataset: `bronze_dart_disclosure_raw` JOIN `silver_dim_symbol` ON stock_code = symbol
   - columns: `rcept_dt`, `corp_name`, `report_nm`, `flr_nm`
   - filter: `dt >= TODAY() - INTERVAL '14' DAYS`, order by `rcept_dt DESC`
   - 공시 영향이 VWAP/거래량에 어떻게 반영되는지 발표 시 cross-reference

## Athena 비용 가드레일

- `tickberg-wg` workgroup 의 쿼리당 최대 스캔 = 5GB (CLAUDE.md 가드)
- Superset dataset 등록 시 partition 필터를 권장 default 로 설정 (`dt >= TODAY() - 7`)
- Dashboard auto-refresh 비활성 (수동 새로고침) — Athena polling 폭주 방지
- SQLLAB row limit 10,000 / timeout 300s (`superset_config.py`)

## 발표용 공유

- 발표 직전 `docker compose up -d superset` → 화면 공유로 KPI/운영 탭 시연
- 외부 공유 링크는 사용하지 않음 (admin/admin dev 계정 + Athena 비용 노출 회피)

## 5/16 추가 (시간 여유 시)

- Iceberg snapshot 추이 (Athena `silver.kis_tick_clean$snapshots` view)
- Late arrival histogram (`silver_ts - trade_ts_kst`)
- 신용정보원 ingest 추세 (Phase 1B Task 32, 시간 부족 시 cut 가능)

## 트러블슈팅

| 증상 | 원인 | 조치 |
|---|---|---|
| 연결 테스트 시 `NoCredentialsError` | `~/.aws/credentials` 미마운트 또는 profile 불일치 | compose `volumes` 의 `~/.aws:/app/.aws:ro` 확인, env `AWS_PROFILE=default` 확인 |
| Athena 쿼리 timeout | 큰 partition scan | 차트 필터로 `dt` 또는 `silver_ts` 범위 제한 |
| `superset` DB 부재 | first boot 안 됨 | 컨테이너 로그 확인, `init-and-start.sh` step [1/5] 가 idempotent 생성 |
| Chart 가 stale | Athena 결과 캐싱 (SimpleCache 5분) | Chart 우상단 ↻ refresh 또는 `superset_config.py` `CACHE_DEFAULT_TIMEOUT` 단축 |
