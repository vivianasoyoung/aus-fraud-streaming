"""
fraud_consumer.py
-----------------
Consumes from `transactions`, applies fraud rules, persists flagged events
to Postgres with at-least-once + idempotent writes. Malformed messages
are routed to a dead-letter topic instead of crashing the loop.

Exposes Prometheus metrics on METRICS_PORT (default 8000): throughput,
fraud-flag rate, risk score distribution, DLQ rate, processing latency,
and consumer lag per partition.
"""

import json
import logging
import os
import sys
from collections import defaultdict, deque
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Callable

import psycopg2
from psycopg2.pool import SimpleConnectionPool
from kafka import KafkaConsumer, KafkaProducer
from prometheus_client import Counter, Gauge, Histogram, start_http_server
from pydantic import BaseModel, Field, ValidationError

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("fraud_consumer")

KAFKA_TOPIC  = os.getenv("KAFKA_TOPIC", "transactions")
DLQ_TOPIC    = os.getenv("KAFKA_DLQ_TOPIC", "transactions.dlq")
BROKER       = os.getenv("KAFKA_BROKER", "localhost:9092")
METRICS_PORT = int(os.getenv("METRICS_PORT", "8000"))
LAG_CHECK_EVERY_N_MESSAGES = 20

DB_DSN = (
    f"host={os.getenv('PG_HOST', '127.0.0.1')} "
    f"port={os.getenv('PG_PORT', '5433')} "
    f"dbname={os.getenv('PG_DB', 'fraud_detection')} "
    f"user={os.getenv('PG_USER', 'fraud')} "
    f"password={os.environ['PG_PASSWORD']}"  # required, no default
)
pool: SimpleConnectionPool | None = None

# ── Prometheus metrics ────────────────────────────────────────────────────────
MESSAGES_PROCESSED = Counter(
    "fraud_consumer_messages_processed_total", "Total messages processed, by outcome", ["outcome"]
)
DLQ_MESSAGES = Counter(
    "fraud_consumer_dlq_messages_total", "Total messages routed to the dead-letter queue, by reason", ["reason"]
)
RISK_SCORE = Histogram(
    "fraud_consumer_risk_score", "Distribution of computed risk scores for flagged transactions",
    buckets=(10, 20, 30, 40, 50, 60, 70, 80, 90, 100),
)
PROCESSING_LATENCY = Histogram(
    "fraud_consumer_processing_seconds", "Time to evaluate + persist a single message",
)
CONSUMER_LAG = Gauge(
    "fraud_consumer_lag_messages", "Consumer lag in messages, per partition", ["partition"]
)


class Transaction(BaseModel):
    transaction_id:    str
    account_id:        str
    amount:            float = Field(ge=0)
    merchant_category: str
    merchant_state:    str
    channel:           str
    transaction_type:  str
    timestamp:         datetime


# ── Rules table: one place to edit ───────────────────────────────────────────
velocity_window: dict[str, deque[datetime]] = defaultdict(deque)
VELOCITY_THRESHOLD = 5
VELOCITY_WINDOW_S  = 60


def _velocity_breach(txn: Transaction) -> bool:
    window = velocity_window[txn.account_id]
    cutoff = txn.timestamp - timedelta(seconds=VELOCITY_WINDOW_S)
    while window and window[0] < cutoff:
        window.popleft()
    window.append(txn.timestamp)
    return len(window) >= VELOCITY_THRESHOLD


@dataclass
class Rule:
    label:     str
    predicate: Callable[[Transaction], bool]
    points:    int


RULES: list[Rule] = [
    Rule("Large amount (>$9k)",          lambda t: t.amount > 9000,                                                          50),
    Rule("Elevated amount (>$5k)",       lambda t: t.amount > 5000,                                                          20),
    Rule("Overseas transaction",         lambda t: t.merchant_state == "OVS",                                                30),
    Rule("Online + odd hour + large",    lambda t: t.channel == "ONLINE" and t.amount > 2000 and (t.timestamp.hour < 6 or t.timestamp.hour > 22), 20),
    Rule("High velocity (5+ in 60s)",    _velocity_breach,                                                                   30),
]


def evaluate(txn: Transaction) -> tuple[list[str], int]:
    fired = [r for r in RULES if r.predicate(txn)]
    reasons = [r.label for r in fired]
    score = min(sum(r.points for r in fired), 100)
    return reasons, score


# ── Persistence ──────────────────────────────────────────────────────────────
def persist(txn: Transaction, reasons: list[str], score: int) -> None:
    conn = pool.getconn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO fraud.flagged_transactions
                    (transaction_id, account_id, amount, merchant_category,
                     channel, fraud_reasons, risk_score, event_time, processed_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (transaction_id) DO NOTHING
                """,
                (
                    txn.transaction_id, txn.account_id, txn.amount,
                    txn.merchant_category, txn.channel,
                    reasons, score, txn.timestamp,
                    datetime.now(timezone.utc),
                ),
            )
        conn.commit()
    finally:
        pool.putconn(conn)


def _update_lag_metric(consumer: KafkaConsumer) -> None:
    """Best-effort consumer lag per partition. Never allowed to crash the main loop."""
    try:
        partitions = consumer.assignment()
        if not partitions:
            return
        end_offsets = consumer.end_offsets(partitions)
        for tp in partitions:
            position = consumer.position(tp)
            lag = max(end_offsets[tp] - position, 0)
            CONSUMER_LAG.labels(partition=str(tp.partition)).set(lag)
    except Exception:  # noqa: BLE001
        log.exception("Failed to compute consumer lag; skipping this cycle")


# ── Main loop ────────────────────────────────────────────────────────────────
def main() -> None:
    global pool
    pool = SimpleConnectionPool(1, 5, dsn=DB_DSN)

    start_http_server(METRICS_PORT)
    log.info("Metrics exposed on :%d/metrics", METRICS_PORT)

    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=BROKER,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        group_id="fraud-detection-group",
    )
    dlq = KafkaProducer(
        bootstrap_servers=BROKER,
        value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
    )

    log.info("Listening on %s", KAFKA_TOPIC)

    processed_count = 0
    for msg in consumer:
        with PROCESSING_LATENCY.time():
            try:
                txn = Transaction.model_validate(msg.value)
                reasons, score = evaluate(txn)
                if reasons:
                    persist(txn, reasons, score)
                    RISK_SCORE.observe(score)
                    MESSAGES_PROCESSED.labels(outcome="flagged").inc()
                    log.warning("FRAUD acc=%s $%.2f score=%d %s",
                                txn.account_id, txn.amount, score, " | ".join(reasons))
                else:
                    MESSAGES_PROCESSED.labels(outcome="ok").inc()
                    log.info("ok    acc=%s $%.2f", txn.account_id, txn.amount)
                consumer.commit()
            except ValidationError as e:
                log.error("Schema error → DLQ: %s", e)
                DLQ_MESSAGES.labels(reason="validation").inc()
                dlq.send(DLQ_TOPIC, value={"error": "validation", "detail": str(e), "raw": msg.value})
                consumer.commit()
            except psycopg2.Error as e:
                log.error("DB error, NOT committing offset: %s", e)
                MESSAGES_PROCESSED.labels(outcome="db_error_retry").inc()
                # No consumer.commit() → message will be redelivered
            except Exception as e:  # noqa: BLE001
                log.exception("Unhandled error → DLQ: %s", e)
                DLQ_MESSAGES.labels(reason="unhandled").inc()
                dlq.send(DLQ_TOPIC, value={"error": "unhandled", "detail": str(e), "raw": msg.value})
                consumer.commit()

        processed_count += 1
        if processed_count % LAG_CHECK_EVERY_N_MESSAGES == 0:
            _update_lag_metric(consumer)


if __name__ == "__main__":
    if "PG_PASSWORD" not in os.environ:
        print("PG_PASSWORD env var required", file=sys.stderr)
        sys.exit(1)
    main()
