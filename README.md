# CBA Banking Data Platform — Project Overview

---

## How this fits with the rest of the project

This repo is one of four that together make up an end-to-end banking data
platform. Built as a portfolio piece demonstrating the modern data /
analytics engineering stack on a single coherent dataset.

```
        ┌───────────────────────────┐
        │  cba-banking-pipeline     │  Airflow + Postgres
        │  → ingests raw txns       │  Source of truth
        └────────────┬──────────────┘
                     │ raw.transactions, raw.accounts
        ┌────────────┴───────────────────────────────┐
        ▼                                            ▼
┌──────────────────────┐                  ┌─────────────────────────┐
│ cba-dbt-analytics    │                  │ cba-fraud-streaming     │
│ → staging / int /    │                  │ → Kafka + rule-based    │
│   marts (dbt)        │                  │   real-time fraud       │
│ → BI / analytics     │                  │ → fraud.flagged_txns    │
└──────────────────────┘                  └────────────┬────────────┘
                                                       │ LABELS
                                                       ▼
                                          ┌─────────────────────────┐
                                          │ cba-feature-store       │
                                          │ → Feast + MLflow        │
                                          │ → /score (FastAPI)      │
                                          └─────────────────────────┘
```

| Repo | Stack | Role | Status |
| --- | --- | --- | --- |
| [`cba-banking-pipeline`](https://github.com/vivianasoyoung/cba-banking-pipeline) | Airflow, Postgres, Docker | Foundation: data generation + ingestion + dbt orchestration | |
| [`cba-dbt-analytics`](https://github.com/vivianasoyoung/cba-dbt-analytics) | dbt-postgres, dbt_utils | Staging → intermediate → marts; segmentation + trends | |
| [`cba-fraud-streaming`](https://github.com/vivianasoyoung/cba-fraud-streaming) | Kafka, Python, Postgres | Real-time rule-based fraud detection |**You are here**|
| [`cba-feature-store`](https://github.com/vivianasoyoung/cba-feature-store) | Feast, MLflow, FastAPI | ML feature store + model serving; labels sourced from streaming | |

### Boundaries

- **Pipeline ↔ dbt:** ingestion writes to `cba_pipeline.raw.*`; dbt reads
  those as sources. Single owner of transformations is `cba-dbt-analytics`.
- **Pipeline ↔ Streaming:** independent — streaming uses its own synthetic
  generator, not the pipeline's CSV. Done deliberately to demonstrate two
  different ingestion styles (batch vs. event).
- **Streaming → Feature Store:** the streaming engine's `fraud.flagged_transactions`
  table is exported and used as ML **labels** in the feature store repo.
  This decouples feature definition from label definition.

### Running the whole thing locally

Each repo is independently runnable via `docker compose up`. To wire them
together end-to-end:

1. `cba-banking-pipeline`: generate data, boot Airflow, let the DAG run once.
2. `cba-dbt-analytics`: `dbt build` against the same Postgres.
3. `cba-fraud-streaming`: `docker compose up` → producer + consumer run.
4. `cba-feature-store`: export flagged txns from the streaming Postgres,
   then `compute_features.py → train_model.py → uvicorn`.
