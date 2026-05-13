FROM apache/superset:4.0.2

USER root

# Athena 연결용 PyAthena[SQLAlchemy] + boto3 (이미 base 에 있지만 명시).
RUN pip install --no-cache-dir \
    "PyAthena[SQLAlchemy]==3.8.0" \
    "boto3>=1.34"

# Init + start 스크립트
COPY init-and-start.sh /app/init-and-start.sh
RUN chmod +x /app/init-and-start.sh

# AWS credentials 마운트 경로 명시
ENV AWS_SHARED_CREDENTIALS_FILE=/app/.aws/credentials \
    AWS_CONFIG_FILE=/app/.aws/config

USER superset
CMD ["/app/init-and-start.sh"]
