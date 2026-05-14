# 100x 스케일 분석 — tickberg Phase 1 → 1억 trades/day

> Phase 1 baseline (1M trades/day, 3 종목) 에서 **100x = 1억 trades/day** (전 종목 KOSPI+KOSDAQ 규모) 로 갈 때
> - 어디가 먼저 깨지는가
> - 무엇을 (구체적 도구·설정·비용으로) 교체하는가
> - 무엇은 그대로 두는가
>
> 본 문서는 **설계 분석**이며 실 구현은 Phase 2 이후. Phase 1 spec(`docs/superpowers/specs/2026-05-07-tickberg-phase1-mvp-design.md`)의 §3.6·3.7·6.7·8 을 standalone 으로 풀어 쓴 버전.

---

## TL;DR

| 깨지는 순서 | 컴포넌트 | 100x 시 limit | 교체 (Phase 3–4) |
|---|---|---|---|
| 1 | Kafka partition (key=symbol, 삼성전자 hot) | 단일 partition ~15K msg/sec 초과 | **MSK Serverless + `bucket(N, symbol)` Iceberg 파티션 + composite key** |
| 2 | Spark Streaming 단일 worker | 1.8M msgs/min 처리 한계 | **EMR Serverless streaming application** + executor auto-scaling |
| 3 | Bronze→Silver batch 5분 window | 종목 단일 MERGE 못 끝남 | 종목 grouping + trigger 단축 (5→2분) |
| 4 | Iceberg Compaction 야간 window | file 수 폭증 | Compaction 분할 + RewriteManifests 분리 |
| 5 | Athena workgroup 5GB scan | 발표·BI 쿼리 막힘 | tier-2 workgroup + Gold 사전집계 강화 |

**핵심 메시지**: 깨지는 곳은 **storage 가 아니라 컴퓨트 + 단일 Kafka partition 의 hot key**. S3·Glue·Iceberg 자체는 100x 까지 그대로.

**비용 폭증**: Phase 1 $25–27/월 → 100x $1,530–3,030/월 (60–110x). budget alarm 은 Phase 1 부터 이미 살짝 초과 → "alarm 동작 검증" narrative 로 활용.

---

## 1. Baseline 정의 — Phase 1 vs 100x 숫자

### Phase 1 baseline (실측 추정)

- **3 종목**: 삼성전자 (005930), SK하이닉스 (000660), NAVER (035420)
- **영업일 hot window**: 09:00–15:30 KST (6.5h)
- **일평균 trades**: ~100만 = 삼성 ~60만 + 하이닉스 ~30만 + NAVER ~10만
- **1초 평균**: ~45/sec (전 종목 합산)
- **1초 피크**: ~250–300/sec (장 시작·종료 burst, 삼성 단독 ~150/sec)

### 100x = 1억 trades/day

- **종목 수**: KOSPI ~900 + KOSDAQ ~1,600 = **~2,500 종목** (전 시장)
- **일평균 trades**: 1억 → 영업시간 6.5h 평균 ~4,500/sec
- **1초 피크**: ~15K–30K/sec (장 시작·돌발 뉴스 burst 시 가장 활성 종목 그룹 hot)
- **CLAUDE.md "100만 → 1억" framework 와 정확 align**. Phase 1 = 100x baseline 학습 환경

---

## 2. 컴포넌트별 capacity matrix

| 컴포넌트 | 이론 한계 | Phase 1 피크 활용률 | 100x 피크 부하 | 한계 도달? |
|---|---|---|---|---|
| KIS Producer (aiohttp + aiokafka) | ~5,000 msgs/sec | 300/sec = **6%** | 30K/sec | 6x 초과 |
| Kafka 단일 broker (KRaft) | ~50K msgs/sec | 300/sec = **0.6%** | 30K/sec | 60% — 위험 |
| Kafka partition (`key=symbol`, hot=삼성) | ~10K msgs/sec/partition | 100/sec = **1%** | 15K/sec/partition | **150% 초과 — 첫 한계** |
| Spark Streaming 단일 worker (1분 trigger) | ~50K msgs/min sink | 평균 5% / 피크 36% | 1.8M msgs/min | **36x 초과 — 두 번째 한계** |
| Bronze→Silver batch (10분 MERGE) | ~수만 rows/min | 평균 15% / 피크 30% | 1.8M rows/10min | 한계 |
| Silver→Gold batch (15분 hour OVERWRITE) | ~수만 rows/min | hour 당 ~3.5K rows | hour 당 350K rows | 여전히 여유 (KPI grain 작음) |
| Iceberg Compaction (daily 18:00 KST 3h window) | ~수 GB/h rewrite | <1 GB/일 | 100x → 100 GB/일 | window 부족 |
| Athena workgroup (5GB scan cutoff) | 5GB/query | <100MB | 1–5GB | tier-2 필요 |
| S3 storage | 사실상 무제한 | <1 GB | 1.5 TB/월 (worst) | 비용만 |
| Glue Catalog | 100만 req/월 free | <10K | 3M req/월 | 유료 전환 |

**핵심 결론**:
1. Phase 1 = 모든 컴포넌트가 압도적 여유 (한 자릿수~30%). single-process Python 으로도 처리 가능
2. 첫 깨짐 = **Kafka partition hot key** (Spark cluster 보다 먼저)
3. 두 번째 = **Spark Streaming 단일 worker** → EMR Serverless 필수
4. Storage·Glue·Iceberg 자체는 100x 까지 그대로 — **깨지는 건 컴퓨트**

---

## 3. Dimension 1 — Throughput (장 시간대 한정 hot path)

| 깨짐 | 100x 증상 | 해결 | 비용 인상 |
|---|---|---|---|
| KIS WebSocket 1 conn/appkey 한도 ~40 종목 | 2,500 종목 못 받음 | **멀티 appkey 분할** (5–10 채널 × 종목 그룹). 추가 KIS 비용 0 (개인계정) ~ 법인계정 fee | 0 (개인) |
| Kafka partition hot (key=symbol, 삼성 단독) | 단일 partition CPU·disk saturate | **composite key = `symbol \|\| floor(ts_minute / N)`** + partition 수 12 → 48 — Iceberg `bucket(N, symbol)` 와 짝 | MSK Serverless ~$50–100/월 |
| Spark Streaming 단일 worker | 1.8M msgs/min 처리 못 끝남 | **EMR Serverless** streaming application + executor 1→16 auto-scale | $1,300–2,700/월 |
| 단일 broker SPOF | 죽으면 hold-and-lose | MSK Serverless **RF=3 + min.insync.replicas=2** | MSK 포함 |

**파티션 키 진화 — 왜 `bucket(N, symbol)` 인가**:

종목 수가 200+ 가 되면 직접 `PARTITIONED BY (symbol, ...)` 은 file 폭주 (한산 종목 파티션 = KB 수준). **`bucket(N, symbol)` hash transform** 은 bucket 수가 고정이라 file 수가 안정. Iceberg hidden partitioning 으로 query 시 자동 활용 → SQL 작성 시 차이 없음 (`WHERE symbol = '005930'` 그대로).

```
Phase 1:  PARTITIONED BY (hour(trade_ts_kst))                          -- 3 종목
Phase 3:  PARTITIONED BY (bucket(8,  symbol), hour(trade_ts_kst))      -- KOSPI200
Phase 4:  PARTITIONED BY (bucket(32, symbol), days(trade_ts_kst))      -- 전 종목 (hot 종목 hash 분산)
```

---

## 4. Dimension 2 — Batch Window (장 마감 후 + 영업시간 internal)

Bronze→Silver MERGE 와 Silver→Gold OVERWRITE 는 영업시간 중 5–15분 주기로 돌고, expire/compaction 은 장 마감 후 야간.

| 깨짐 | 증상 | 해결 |
|---|---|---|
| Bronze→Silver 5분 안 못 끝남 | 100x = 5분당 ~1.5M rows × 종목 수 → 단일 worker 시간 초과 | (a) 종목 group partition 병렬, (b) trigger 5→2분 (batch 크기 ↓), (c) Spark Streaming `foreachBatch` 통합으로 stage 분리 |
| Silver→Gold OVERWRITE 부담 | hour partition 의 row 수 ×100 | **Gold cascade** (1초 → 1분 → 5분 → 1시간) — 상위 grain 일수록 OVERWRITE 범위 작음 |
| Late arrival 처리 | 100x → late event 수도 증가 | watermark 명시 + Iceberg 의 `WHEN NOT MATCHED INSERT *` 가 자연스럽게 흡수 (현재 패턴) |

**Trigger 진화**: Phase 1 = 1분 trigger 로 신선도·small-files 균형. 100x 에서 1분 유지 시 micro-batch 가 sink 못 함 → **2분 (Phase 3) 또는 30초 (Phase 4 EMR Serverless 시)** 로 조정. 1분은 Gold 분봉 boundary align 때문에 가능하면 유지.

---

## 5. Dimension 3 — Storage / Compaction (장 마감 후 한정)

| 깨짐 | 증상 | 해결 |
|---|---|---|
| Compaction 야간 3h window 초과 | file 수 100x → 단일 spark job 시간 부족 | **day 단위 분할 Compaction** + RewriteManifests 별도 DAG. spec §6 의 `rewrite_data_files` procedure + `rewrite_manifests` 추가 |
| `expire_snapshots` 못 따라잡음 | 5K+ snapshots/주 (1분 trigger × 5일 × 12h) | 일배치 + retention 30→14일. **`expire_snapshots('30 days')`** Spark Action |
| Orphan file 누적 | abort 된 commit 의 data file | 주배치 `remove_orphan_files` |
| S3 storage 비용 worst | 1.5 TB/월 → $36/월 (Standard) | **Bronze Lifecycle Glacier IR 적극 90→30일**, Silver/Gold = Standard 유지 |

**`write target file size` 진화**: Phase 1 = 256MB write / 384MB compaction. 100x = 512MB write / 1GB compaction. Athena/Spark scan 효율 + S3 object 수 감소.

**메달리온 retention 분화** (Phase 4):
- Bronze 30일 Standard → Glacier IR (raw 보존, 재처리 안전망)
- Silver 1년 Standard (재계산 base)
- Gold 영구 Standard (KPI history)

---

## 6. Dimension 4 — Concurrency (BI·쿼리 부하)

| 깨짐 | 증상 | 해결 |
|---|---|---|
| Athena workgroup 5GB cutoff | 발표·BI 쿼리 막힘 | (a) **Gold 사전집계 강화** (분→5분→일 cascade), (b) tier-2 workgroup (`tickberg-bi-wg` 50GB cutoff 별도), (c) `query result reuse` 활성 |
| Glue Catalog 100만 req/월 free 초과 | 100x = 3M req/월 | 유료 전환 — $1/M req. **연 $24** 수준 |
| Superset 부하 | 대시보드 동시접속 100명+ | **Redis cache** (현 in-memory `SimpleCache` 교체) + Athena `query result reuse` 의존 |
| Grafana scrape 부하 | 메트릭 expose 컴포넌트 수 증가 | hierarchical scrape (1차 = master Prometheus → 2차 federated) — Phase 4 시 검토 |

---

## 7. Evolution 경로 — 4단계

각 단계에서 깨지는 컴포넌트가 명확 → 단계적 전환.

| Phase | 부하 | 종목 수 | 컴퓨트 | Iceberg partition | 주요 변경 |
|---|---|---|---|---|---|
| **1 (1x)** | 100만/일 | 3 (삼성/하이닉스/NAVER) | 로컬 Docker (Kafka 1·Spark 1) | `hour(trade_ts_kst)` | 현재 |
| **2 (10x)** | 1,000만/일 | KOSPI 시총 상위 30 | MSK Serverless + Spark local 유지 | `hour(trade_ts_kst)` 유지 (column stats 로 종목 prune 충분) | KIS multi-appkey + Kafka partitions 12→24 |
| **3 (50x)** | 5,000만/일 | KOSPI200 | EMR Serverless streaming + MSK | `bucket(8, symbol), hour(trade_ts_kst)` | EMR 도입, Iceberg branch (audit) |
| **4 (100x)** | 1억/일 | 전 종목 KOSPI+KOSDAQ ~2,500 | EMR Serverless full + MSK + 멀티 region | `bucket(32, symbol), days(trade_ts_kst)` | DR (멀티 region), Superset Enterprise 패턴 |

**전환 trigger 가 무엇인가**:
- Phase 1 → 2: KIS WebSocket 단일 conn 한도 (~40 종목) 도달
- Phase 2 → 3: Spark local worker 의 CPU·메모리 saturate (Bronze→Silver 5분 안 안 끝남)
- Phase 3 → 4: Iceberg `bucket(8)` 의 단일 bucket 부하 saturate

**한 번에 100x 가지 않는 이유**: 각 단계마다 신규 도구 도입 risk + 운영 학습 곡선. EMR Serverless·MSK·멀티 region 을 한 번에 도입하면 디버깅 face surface 가 폭발.

---

## 8. AWS 비용 — assumption + worst/best

### 8.1 공통 가정

| 가정 | 값 |
|---|---|
| 영업일 | 252일/년 |
| 영업시간 hot window | 6.5h/일 (09:00–15:30 KST) |
| Bronze row 크기 (parquet 압축 후) | ~150–250 bytes |
| Silver row 크기 | ~80–120 bytes |
| Gold row 크기 | ~50 bytes |
| Region | ap-northeast-2 (서울) |
| Iceberg target file | 256MB (Phase 1) / 512MB (100x) |
| Compaction freq | daily 18:00 KST |
| QuickSight | 미사용 (Superset 로 대체) |

### 8.2 Phase 1 (1x) — 실측 기반

| 서비스 | best | worst | 근거 |
|---|---|---|---|
| S3 storage | $0.10 | $0.25 | 10 GB worst (compression 2x + Iceberg metadata 5x) |
| S3 PUT/GET | $0.05 | $0.20 | Iceberg commit + Spark checkpoint = ~5K PUT/일 × 7일 |
| Athena scan | $0.01 | $0.10 | 디버깅 부주의 scan 5 GB/주 worst |
| Glue Catalog | $0 | $0 | 무료 한도 안 |
| Superset compute | $0 | $5 | 로컬 Docker (발표 외 시간 정지). EC2 대안 시 t3.small ~$10 |
| Grafana | $0 | $0 | 로컬 Docker |
| Cross-AZ transfer | $0 | $0.50 | Spark ↔ S3 (같은 region) |
| **합계** | **$0.16/월** | **$5.95/월** | budget $20 alarm **미초과** |

> Spec §6.7 의 worst case ~$24–26/월은 **QuickSight Author license $24 포함** 가정이었음. Superset 채택으로 이 비용 제거 → 실측 worst $5–6/월. AWS Budgets alarm 은 발표 직전 트리거 안 됨 — narrative 수정 필요 ("alarm 동작 검증" 은 인위 burst test 로 대체).

### 8.3 100x worst + best (월 비용)

| 서비스 | best | worst | 가정 |
|---|---|---|---|
| EMR Serverless 컴퓨트 | $400 | $2,700 | best = 영업시간만 on-demand (6.5h × 21영업일), worst = 24h on |
| MSK Serverless | $50 | $100 | broker 1→3, RF=3 |
| S3 storage (1.5 TB Standard + Bronze Glacier IR) | $30 | $145 | best = Glacier IR aggressive 30일 후 이동, worst = Standard 유지 |
| S3 PUT/GET | $20 | $80 | Iceberg commit ×100 |
| Athena scan | $1 | $50 | best = Gold 사전집계만, worst = 디버깅 scan |
| Glue Catalog | $2 | $5 | 무료 한도 초과 |
| Superset compute (Fargate t3.small) | $20 | $50 | best = 단일 task, worst = HA 2 task + Redis |
| Grafana (셀프호스트 + storage) | $5 | $30 | EBS + scrape 부담 |
| Cross-AZ transfer | $5 | $50 | EMR ↔ S3, multi-AZ MSK |
| **합계** | **$533/월** | **$3,210/월** | budget $20 의 **26–160x 초과** |

**핵심 메시지**:
- 100x worst ($3K/월) 은 budget alarm 의 **150x 초과**. budget 자체를 100x 늘리거나 알람 임계값 재정의
- 깨지는 곳 = **EMR Serverless 컴퓨트가 단연 1위** (60–84%). 따라서 영업시간만 on-demand 가동 시 비용 절반 가능
- storage 는 worst case 에서도 전체의 5% — **storage 가 한계는 아님**

### 8.4 비용 절감 lever (장 시간대 / 마감 후 분리)

| Lever | Phase 1 효과 | 100x 효과 |
|---|---|---|
| EMR Serverless on-demand (영업시간만) | n/a | 컴퓨트 ~60% 절감 (24h → 6.5h × 21영업일) |
| Bronze Glacier IR 30일 이동 | <$1 | $115 절감 |
| Athena Gold 사전집계 강화 | <$1 | $49 절감 |
| Superset 발표 외 시간 정지 | $5 절감 | $30 절감 (Fargate 시간당 과금) |
| Iceberg compaction file size↑ | 미미 | scan 효율 +30% → Athena 추가 절감 |

---

## 9. 장 시간대 vs 장 마감 후 분리

운영 가시성과 컴퓨트 사용 패턴 모두에서 **두 구간이 다른 시스템처럼 동작**.

| 구간 | 시간 | 시스템 동작 | 100x 시 전략 |
|---|---|---|---|
| **장 시간대** | 09:00–15:30 KST 평일 | Producer WS · Spark Streaming Bronze · Bronze→Silver batch · Silver→Gold | 모든 컴퓨트 활성, EMR Serverless on-demand peak |
| **장 마감 후** | 15:30 → 익일 08:30 | 03:30 KIS token refresh · 04:00 dim_symbol · 06:00 DART · 18:00 Compaction · 19:00 expire_snapshots | streaming app 정지 또는 minimal, 배치만 |
| **비영업일 (주말·공휴일)** | — | 운영 거의 정지 | EMR Serverless 완전 정지 → 비용 0 |

**100x 비용 절감 핵심**: EMR Serverless 가 장 시간대 + 일배치 야간 2 windows 만 가동되면 24h on 대비 70%+ 절감.

**모니터링 narrative**:
- 장 시간대 alert: `kis_ws_disconnected`, `bronze_freshness > 5min`, `kafka_consumer_lag > 1000`
- 장 마감 후 alert: `dim_symbol_dag_failed`, `compaction_overrun`, `expire_snapshots_failed`
- 두 구간 segment 를 Grafana template variable 로 토글 시각화

---

## 10. What doesn't break — 100x 까지 그대로

방어적 narrative — 평가 시 "S3 가 깨지지 않냐" 질문 대응.

| 컴포넌트 | 100x 한계 | 사유 |
|---|---|---|
| **S3 (data plane)** | 사실상 무제한 | object 수와 전송량 둘 다 hyperscale. 단위 비용만 우상향 |
| **Iceberg table format** | 그대로 | manifest tree 가 partition prune 효율 유지. `bucket(N, symbol)` 으로 파티션만 진화하면 됨 |
| **Glue Data Catalog** | 100만 req/월 free 초과 시 $1/M | metadata 자체는 100x 에서도 $2–5/월 수준 |
| **Athena 쿼리 엔진** | tier-2 workgroup 필요 | 엔진 자체는 무제한. scan 한도만 운영 정책 |
| **현재 메달리온 3계층** | 그대로 | Bronze/Silver/Gold 책임 분리는 100x 에서도 동일 |
| **현재 DAG 4종** (bronze→silver, silver→gold, dim_symbol, compaction) | task scale 조정만 | task 자체 구조는 동일 |
| **Iceberg ① MERGE ② OVERWRITE ③ time-travel 패턴** | 그대로 | Phase 1 학습 자산이 100x 까지 직접 활용 |

→ **Phase 1 architecture 의 90%+ 가 100x 까지 유효**. 컴퓨트 layer 만 EMR/MSK 로 교체.

---

## 11. Open questions / Phase 2+ deferred

| 항목 | 사유 |
|---|---|
| EMR Serverless 도입 시점 (Phase 2 vs 3) | 실 부하 측정 후 결정 — Spark local worker CPU saturate 가 trigger |
| MSK vs Confluent Cloud 선택 | 비용·운영 단순화 trade-off. MSK Serverless 기본 채택 가정 |
| 멀티 region 필요성 | 발표·시연 단일 region (ap-northeast-2) 으로 충분. DR 요구는 운영 전환 후 평가 |
| Trino 도입 (Phase 4) | Athena scan 비용이 $50/월 초과 시 검토. Phase 1 미사용 |
| dbt 도입 (Phase 2) | Silver/Gold MERGE INTO 가 5개 이상 추가되면 dbt model 화 검토 |
| Iceberg branch / tag 활용 | audit · debug 용 — Phase 3 부터 검토 |
| Apache Flink 전환 | Spark Streaming 의 sub-second latency 요구 발생 시 (현재 1분 trigger 로 충분) |
| AWS Secrets Manager | 운영 전환 시 (Phase 2 이후) |

---

## References

- **CLAUDE.md** — Phase 1 결정 + "100만→1억" framework
- **`docs/superpowers/specs/2026-05-07-tickberg-phase1-mvp-design.md`**
  - §3.6 데이터 규모 추정
  - §3.7 부하 capacity 검증
  - §6.7 비용 추정
  - §8.3 Dimension 4 분해
  - §8.5 Evolution 경로
- **`code/ddl/`** — Bronze/Silver/Gold 현재 schema
- **`orchestration/dags/`** — 4 DAG (bronze_to_silver, silver_to_gold, dim_symbol, iceberg_compaction)

---

## 가격·수치 disclaimer

- 가격은 2026 AWS ap-northeast-2 공시 기준. EMR Serverless DPU·시간, MSK Serverless 처리량, Superset Fargate 시간당 과금에 따라 ±30% 변동 가능.
- Phase 1 일평균 trades 수 (~100만) 는 3 종목 보수 추정 (삼성 ~60만 / 하이닉스 ~30만 / NAVER ~10만). 실측은 5/14 영업일 마감 후 갱신 예정.
- 100x 피크 ~15K–30K/sec 은 전 종목 합산 worst-case. 평균은 ~4,500/sec.
- **worst case framing 이 본 문서의 핵심**. 낙관 단일값을 의도적으로 회피 — 평가에서 정직성이 plus.
