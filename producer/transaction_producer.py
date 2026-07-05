"""

transaction_producer.py
-----------------------
Simulates a real-time banking transaction stream.
Sends transactions to Kafka, keyed by account_id so all transactions
for one account land on the same partition (required for the velocity
rule to be correct under multiple consumers).

Periodically injects suspicious transactions to exercise the fraud rules.

"""

import json
import os
import random
import time
import uuid
from datetime import datetime, timezone

from kafka import KafkaProducer

KAFKA_TOPIC  = os.getenv("KAFKA_TOPIC", "transactions")
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")

MERCHANT_CATEGORIES = [
    "Supermarkets", "Restaurants", "Fuel", "Online Shopping",
    "Transport", "Utilities", "Healthcare", "Entertainment", "ATM Withdrawal",
]
AU_STATES   = ["NSW", "VIC", "QLD", "WA", "SA", "TAS"]
CHANNELS    = ["EFTPOS", "ONLINE", "ATM", "BPAY"]
ACCOUNT_IDS = [f"ACC{str(i).zfill(7)}" for i in range(1, 201)]


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def normal(account_id: str) -> dict:
    return {
        "transaction_id":    str(uuid.uuid4()),
        "account_id":        account_id,
        "amount":            round(random.uniform(5, 500), 2),
        "merchant_category": random.choice(MERCHANT_CATEGORIES),
        "merchant_state":    random.choice(AU_STATES),
        "channel":           random.choice(CHANNELS),
        "transaction_type":  "DEBIT",
        "timestamp":         _now_iso(),
        "is_suspicious":     False,
    }


def suspicious(account_id: str, kind: str) -> dict:
    txn = normal(account_id)
    txn["is_suspicious"] = True
    if kind == "large_amount":
        txn["amount"] = round(random.uniform(9000, 50000), 2)
        txn["fraud_reason"] = "Large transaction amount"
    elif kind == "rapid_fire":
        txn["fraud_reason"] = "Rapid successive transactions"
    elif kind == "overseas_night":
        txn["merchant_state"] = "OVS"
        txn["channel"] = "ONLINE"
        txn["amount"] = round(random.uniform(2500, 8000), 2)
        txn["fraud_reason"] = "Overseas transaction outside business hours"
    return txn


def build_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        acks="all",     # don't ack until all in-sync replicas have it
        retries=5,      # at-least-once; kafka-python has no idempotent producer support
    )


def main() -> None:
    producer = build_producer()
    print(f"Producing to {KAFKA_TOPIC} via {KAFKA_BROKER}. Ctrl+C to stop.")

    txn_count = 0
    rapid_account = None
    rapid_remaining = 0

    while True:
        account_id = random.choice(ACCOUNT_IDS)

        if rapid_remaining > 0:
            txn = suspicious(rapid_account, "rapid_fire")
            rapid_remaining -= 1
            print(f"RAPID  acc={rapid_account} ${txn['amount']} ({rapid_remaining} left)")
            sleep_s = 0.05  # actually rapid
        elif txn_count > 0 and txn_count % 20 == 0:
            kind = random.choice(["large_amount", "rapid_fire", "overseas_night"])
            if kind == "rapid_fire":
                rapid_account = account_id
                rapid_remaining = random.randint(5, 8)
            txn = suspicious(account_id, kind)
            print(f"SUSP   [{kind}] acc={account_id} ${txn['amount']}")
            sleep_s = random.uniform(0.5, 1.5)
        else:
            txn = normal(account_id)
            print(f"ok     acc={account_id} ${txn['amount']} {txn['merchant_category']}")
            sleep_s = random.uniform(0.5, 1.5)

        producer.send(KAFKA_TOPIC, key=account_id, value=txn)
        txn_count += 1
        time.sleep(sleep_s)


if __name__ == "__main__":
    main()
