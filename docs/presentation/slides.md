# PortfolioStream — Real-time Korean Stock Tick Lakehouse

> tickberg 최종 발표 자료. 33장 슬라이드, 발표 25분.
> Gamma 입력용 단일 MD. 슬라이드 구분은 `---`.

---

## 01. 표지

> PortfolioStream — Real-time Korean Stock Market Tick Data Lakehouse

- 메타코드 DE 부트캠프 8회차 최종 프로젝트
- 한국투자증권 실시간 체결 + DART + 신용정보원
- S3 Iceberg Lakehouse + Superset + Prometheus/Grafana
- 발표자 / 발표일 2026-05-16

**시각자료**: 표지 — 프로젝트명 + 한 줄 소개 + 발표자명. 디자인은 Gamma 단계에서.

**Speaker Note:**
안녕하세요. 오늘 발표할 프로젝트는 PortfolioStream, 한국 주식 실시간 체결 데이터를 위한 Lakehouse 입니다.
한국투자증권 Open API 에서 들어오는 체결 데이터를 Bronze, Silver, Gold 메달리온 구조로 적재하고,
이걸 BI 대시보드와 운영 가시성으로 묶는 게 이번 Phase 1 의 미션입니다.
오늘 25분 동안 핵심 결정 5가지, Iceberg 가 왜 필요했는지, 운영을 어떻게 보고 있는지를 보여드리고
마지막에 100배 트래픽 시나리오에 대한 사고 흐름과 Phase 2 로드맵을 공유하겠습니다.

---
