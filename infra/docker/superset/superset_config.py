"""Superset config — Airflow Postgres 공유, in-memory cache, Athena 드라이버 활성화."""
import os

# === Metadata DB: Airflow Postgres 의 별도 database `superset` ===
# Postgres 의 'airflow' superuser 로 같은 인스턴스의 'superset' DB 사용.
# (compose 의 airflow-postgres 컨테이너 = host 'airflow-postgres' / port 5432)
SQLALCHEMY_DATABASE_URI = os.environ.get(
    "SUPERSET_METADATA_URI",
    "postgresql+psycopg2://airflow:airflow@airflow-postgres:5432/superset",
)

# === Secret key ===
# 운영에선 env 로 강제 주입. dev 디폴트는 약하니 별도 SECRET_KEY env 필수.
SECRET_KEY = os.environ.get("SUPERSET_SECRET_KEY", "dev-key-change-me")

# === Feature flags ===
FEATURE_FLAGS = {
    "DASHBOARD_NATIVE_FILTERS": True,
    "DASHBOARD_CROSS_FILTERS": True,
    "ENABLE_TEMPLATE_PROCESSING": True,
    "SQLLAB_BACKEND_PERSISTENCE": True,
}

# === Athena 워크그룹 / 결과 path 기본값 (UI 에서 datasource 추가 시 사용) ===
ATHENA_S3_STAGING_DIR = os.environ.get(
    "ATHENA_S3_STAGING_DIR",
    "s3://tickberg-lakehouse/athena-results/",
)
ATHENA_WORK_GROUP = os.environ.get("ATHENA_WORK_GROUP", "tickberg-wg")

# === Row limit (Athena 5GB cutoff 보완) ===
ROW_LIMIT = 10_000
DEFAULT_SQLLAB_LIMIT = 10_000
SQLLAB_TIMEOUT = 300

# === 캐시 (Phase 1 는 메모리. Redis 도입 시 변경) ===
CACHE_CONFIG = {
    "CACHE_TYPE": "SimpleCache",
    "CACHE_DEFAULT_TIMEOUT": 300,
}
