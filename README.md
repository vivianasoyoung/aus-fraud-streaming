# Australian Banking Fraud Streaming Pipeline

A real-time fraud detection pipeline that processes banking transactions as they happen using Apache Kafka. Modelled on how banks detect fraudulent activity within seconds of a transaction.

> **Disclaimer:** Personal learning project built with entirely synthetic, programmatically generated data. Not affiliated with, endorsed by, or using systems, schemas, or data from any financial institution.

## How this fits with the rest of the project

| Repo | Stack | Role |
| --- | --- | --- |
| [`aus-banking-pipeline`](https://github.com/vivianasoyoung/aus-banking-pipeline) | Airflow, Postgres, Docker | Foundation: synthetic data generation + batch ingestion |
| [`aus-dbt-analytics`](https://github.com/vivianasoyoung/aus-dbt-analytics) | dbt-postgres, dbt_utils | Staging → intermediate → marts transformations |
| **[`aus-fraud-streaming`](https://github.com/vivianasoyoung/aus-fraud-streaming)** *(You are here)* | Kafka, Python, Postgres | Real-time rule-based fraud detection. Produces the **labels** consumed by `aus-feature-store`. |
| [`aus-feature-store`](https://github.com/vivianasoyoung/aus-feature-store) | Feast, MLflow, FastAPI | ML feature store + model serving |

The `fraud.flagged_transactions` table this repo populates is used as **ML labels** in `aus-feature-store` — decoupling label definition from feature definition.

---

## Architecture

```
Transaction Producer (Python, containerised)
        │  keyed by account_id (one partition per account)
        ▼
Kafka Topic: "transactions"
        │
        ▼
Fraud Detection Consumer (Python, containerised)
   ├── Pydantic schema validation
   ├── Rules engine (5 scored rules)
   ├── Dead-letter queue for malformed messages
   ├── Manual offset commits after successful DB write
   ├── Connection pool
   └── Prometheus metrics on :8000/metrics
        │
        ├──► PostgreSQL — fraud.flagged_transactions (UNIQUE on transaction_id, ON CONFLICT DO NOTHING)
        │
        └──► Prometheus (scrapes every 5s) ──► Grafana (provisioned dashboard)
```

## Tech Stack

| Layer | Tool |
|---|---|
| Message broker | Apache Kafka |
| Stream processing | Python (kafka-python, Pydantic) |
| Storage | PostgreSQL 15 |
| Orchestration | Docker + Docker Compose |
| Monitoring | Kafka UI, Prometheus, Grafana |

## Quick Start

```bash
cp .env.example .env        # set PG_PASSWORD
docker compose up -d        # boots Kafka, Postgres, Kafka UI, producer, consumer, Prometheus, Grafana
docker compose logs -f consumer
```

| Service | URL | Notes |
|---|---|---|
| Kafka UI | http://localhost:8090 | |
| Grafana | http://localhost:3000 | Anonymous viewer access enabled — "Fraud Consumer" dashboard is pre-provisioned, no setup needed. Login `admin`/`admin` for edit access. |
| Prometheus | http://localhost:9090 | Raw metrics + query UI |
| Consumer metrics | http://localhost:8000/metrics | Raw Prometheus exposition format |
| PostgreSQL | localhost:5433 | |

Query flagged transactions:

```bash
docker compose exec postgres psql -U fraud -d fraud_detection \
  -c "SELECT account_id, amount, risk_score, fraud_reasons, event_time \
      FROM fraud.flagged_transactions ORDER BY processed_at DESC LIMIT 10;"
```

## Fraud Detection Rules

| Rule | Condition | Risk Score |
|---|---|---|
| Large amount | > $9,000 | +50 |
| Elevated amount | > $5,000 | +20 |
| Overseas | merchant_state = OVS | +30 |
| Late-night online | ONLINE + > $2,000 + hour < 6 or > 22 | +20 |
| High velocity | 5+ transactions in 60 seconds | +30 |

Score capped at 100. Rules are defined as data (`RULES = [...]`) — one place to read or edit them.

## Observability

The consumer exposes Prometheus metrics, scraped every 5 seconds, visualized
in a Grafana dashboard that's provisioned automatically on startup — no
manual dashboard setup required.

| Metric | What it shows |
|---|---|
| `fraud_consumer_messages_processed_total{outcome}` | Throughput, split by ok / flagged / db_error_retry |
| `fraud_consumer_dlq_messages_total{reason}` | Dead-letter rate, split by validation / unhandled |
| `fraud_consumer_risk_score` | Distribution (p50/p95) of risk scores on flagged transactions |
| `fraud_consumer_processing_seconds` | Per-message processing latency (p99 on the dashboard) |
| `fraud_consumer_lag_messages{partition}` | Consumer lag, recomputed every 20 messages |

The dashboard has 6 panels: throughput by outcome, fraud flag rate, consumer
lag per partition, DLQ rate by reason, risk score percentiles, and p99
processing latency. Suggested alert threshold for consumer lag (not wired to
a pager here, but this is what a real Alertmanager rule would use): lag
sustained above ~100 messages for 5 minutes at this pipeline's throughput
corresponds to roughly the ">30s" threshold mentioned in
[`REAL_WORLD_NOTES.md`](./REAL_WORLD_NOTES.md).

## Robustness

- **Idempotent inserts** — `UNIQUE(transaction_id)` + `ON CONFLICT DO NOTHING`; replays are safe
- **At-least-once delivery** — offsets commit only after the DB write succeeds
- **Dead-letter queue** — malformed messages routed to `transactions.dlq` instead of crashing the loop
- **Schema validation** — Pydantic rejects malformed messages
- **Connection pooling** — instead of connect-per-write

See [`REAL_WORLD_NOTES.md`](./REAL_WORLD_NOTES.md) for what would change at production scale.

## Project Structure

```
aus-fraud-streaming/
├── producer/transaction_producer.py
├── consumer/fraud_consumer.py
├── docker/init.sql
├── docker/prometheus.yml
├── docker/grafana/provisioning/datasources/prometheus.yml
├── docker/grafana/provisioning/dashboards/dashboard.yml
├── docker/grafana/dashboards/fraud-consumer.json
├── Dockerfile.producer
├── Dockerfile.consumer
├── requirements.txt
├── docker-compose.yml
├── .env.example
└── README.md
```
