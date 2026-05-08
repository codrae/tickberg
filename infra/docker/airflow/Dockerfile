FROM apache/airflow:2.9.3-python3.11

USER root
RUN apt-get update \
 && apt-get install -y --no-install-recommends docker.io \
 && rm -rf /var/lib/apt/lists/*
USER airflow
RUN pip install --no-cache-dir apache-airflow-providers-amazon==8.24.0
