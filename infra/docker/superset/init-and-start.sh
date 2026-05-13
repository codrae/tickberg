#!/bin/bash
set -euo pipefail

echo "==> [1/5] Creating 'superset' database in airflow-postgres if missing"
python <<'PY'
import psycopg2
conn = psycopg2.connect(
    host="airflow-postgres", port=5432,
    user="airflow", password="airflow",
    database="airflow",
)
conn.autocommit = True
cur = conn.cursor()
cur.execute("SELECT 1 FROM pg_database WHERE datname='superset'")
if cur.fetchone():
    print("  -> exists")
else:
    cur.execute("CREATE DATABASE superset")
    print("  -> created")
conn.close()
PY

echo "==> [2/5] superset db upgrade"
superset db upgrade

echo "==> [3/5] create admin user (idempotent)"
superset fab create-admin \
    --username admin \
    --firstname Tickberg \
    --lastname Admin \
    --email admin@tickberg.local \
    --password admin || true

echo "==> [4/5] superset init (roles, permissions)"
superset init

echo "==> [5/5] starting gunicorn on :8088"
exec gunicorn \
    --workers 4 \
    --worker-class gthread \
    --threads 8 \
    --timeout 120 \
    --bind 0.0.0.0:8088 \
    'superset.app:create_app()'
