"""Prometheus metrics for KIS producer (T1 Grafana panels).

Exposes:
  kis_ws_connected{}            gauge 0/1
  kis_messages_published_total  counter
  kis_parse_errors_total        counter
  kis_token_refresh_failures_total counter
"""
from __future__ import annotations

from prometheus_client import Counter, Gauge, start_http_server

ws_connected = Gauge("kis_ws_connected", "1 if KIS WebSocket connected, else 0")
messages_published = Counter("kis_messages_published_total", "Tick messages published to Kafka", ["symbol"])
parse_errors = Counter("kis_parse_errors_total", "KIS payload parse errors")
token_refresh_failures = Counter("kis_token_refresh_failures_total", "Token refresh failures")


def start(port: int = 9100) -> None:
    start_http_server(port)
