"""
fraud_consumer.py
-----------------
Consumes transactions from Kafka in real time.
Applies fraud detection rules and stores flagged transactions in Postgres.
"""

import json
import psycopg2
from datetime import datetime, timedelta
from collections import defaultdict
from kafka import KafkaConsumer

KAFKA_TOPIC  = "transactions"
KAFKA_BROKER = "localhost:9092"

DB_CONFIG = {
    "host":     "127.0.0.1",
    "port":     5433,
    "dbname":   "fraud_detection",
    "user":     "fraud",
    "password": "fraud",
}

# In-memory velocity tracker: account_id -> list of timestamps
velocity_tracker = defaultdict(list)

def get_db():
    return psycopg2.connect(**DB_CONFIG)

def calculate_risk_score(txn: dict, reasons: list) -> int:
    score = 0
    if txn["amount"] > 9000:
        score += 50
    if txn["amount"] > 5000:
        score += 20
    if txn.get("merchant_state") == "OVS":
        score += 30
    hour = datetime.fromisoformat(txn["timestamp"]).hour
    if hour < 6 or hour > 22:
        score += 20
    if len(reasons) > 1:
        score += 10
    return min(score, 100)

def check_velocity(account_id: str, timestamp: str) -> bool:
    """Flag if account has 5+ transactions in last 60 seconds."""
    now = datetime.fromisoformat(timestamp)
    cutoff = now - timedelta(seconds=60)
    
    # Clean old entries
    velocity_tracker[account_id] = [
        t for t in velocity_tracker[account_id] if t > cutoff
    ]
    velocity_tracker[account_id].append(now)
    
    return len(velocity_tracker[account_id]) >= 5

def detect_fraud(txn: dict) -> tuple[bool, list, int]:
    reasons = []

    # Rule 1: Large amount
    if txn["amount"] > 9000:
        reasons.append(f"Large amount: ${txn['amount']}")

    # Rule 2: Velocity check
    if check_velocity(txn["account_id"], txn["timestamp"]):
        reasons.append("High velocity: 5+ transactions in 60 seconds")

    # Rule 3: Overseas transaction
    if txn.get("merchant_state") == "OVS":
        reasons.append("Overseas transaction detected")

    # Rule 4: Large online transaction at night
    hour = datetime.fromisoformat(txn["timestamp"]).hour
    if txn["channel"] == "ONLINE" and txn["amount"] > 2000 and (hour < 6 or hour > 22):
        reasons.append(f"Large online transaction at odd hour ({hour}:00)")

    is_fraud = len(reasons) > 0
    risk_score = calculate_risk_score(txn, reasons) if is_fraud else 0
    return is_fraud, reasons, risk_score

def save_flagged_transaction(txn: dict, reasons: list, risk_score: int):
    conn = get_db()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO fraud.flagged_transactions
                    (transaction_id, account_id, amount, merchant_category,
                     channel, fraud_reason, risk_score, flagged_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    txn["transaction_id"],
                    txn["account_id"],
                    txn["amount"],
                    txn["merchant_category"],
                    txn["channel"],
                    " | ".join(reasons),
                    risk_score,
                    datetime.now(),
                )
            )
        conn.commit()
    finally:
        conn.close()

def main():
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BROKER,
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        auto_offset_reset='latest',
        group_id='fraud-detection-group'
    )

    print(f"Fraud detection consumer started → listening on: {KAFKA_TOPIC}")
    print("Waiting for transactions...\n")

    for message in consumer:
        txn = message.value
        is_fraud, reasons, risk_score = detect_fraud(txn)

        if is_fraud:
            save_flagged_transaction(txn, reasons, risk_score)
            print(f"🚨 FRAUD DETECTED | acc={txn['account_id']} "
                  f"amount=${txn['amount']} score={risk_score} | {' | '.join(reasons)}")
        else:
            print(f"✅ clean | acc={txn['account_id']} amount=${txn['amount']}")

if __name__ == "__main__":
    main()
