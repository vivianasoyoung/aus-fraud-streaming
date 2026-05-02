"""
transaction_producer.py
-----------------------
Simulates a real-time banking transaction stream.
Sends random transactions to Kafka every 0.5-2 seconds.
Occasionally injects suspicious transactions to trigger fraud detection.
"""

import json
import random
import time
import uuid
from datetime import datetime
from kafka import KafkaProducer

KAFKA_TOPIC = "transactions"
KAFKA_BROKER = "localhost:9092"

MERCHANT_CATEGORIES = [
    "Supermarkets", "Restaurants", "Fuel", "Online Shopping",
    "Transport", "Utilities", "Healthcare", "Entertainment", "ATM Withdrawal"
]

AU_STATES = ["NSW", "VIC", "QLD", "WA", "SA", "TAS"]
CHANNELS = ["EFTPOS", "ONLINE", "ATM", "BPAY"]

# Sample account IDs
ACCOUNT_IDS = [f"ACC{str(i).zfill(7)}" for i in range(1, 201)]

def generate_normal_transaction(account_id):
    return {
        "transaction_id":   str(uuid.uuid4()),
        "account_id":       account_id,
        "amount":           round(random.uniform(5, 500), 2),
        "merchant_category": random.choice(MERCHANT_CATEGORIES),
        "merchant_state":   random.choice(AU_STATES),
        "channel":          random.choice(CHANNELS),
        "transaction_type": "DEBIT",
        "timestamp":        datetime.now().isoformat(),
        "is_suspicious":    False
    }

def generate_suspicious_transaction(account_id, fraud_type):
    txn = generate_normal_transaction(account_id)
    txn["is_suspicious"] = True

    if fraud_type == "large_amount":
        txn["amount"] = round(random.uniform(9000, 50000), 2)
        txn["fraud_reason"] = "Large transaction amount"

    elif fraud_type == "rapid_fire":
        txn["fraud_reason"] = "Rapid successive transactions"

    elif fraud_type == "overseas_night":
        txn["merchant_state"] = "OVS"
        txn["channel"] = "ONLINE"
        txn["fraud_reason"] = "Overseas transaction outside business hours"

    return txn

def main():
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    print(f"Starting transaction producer → topic: {KAFKA_TOPIC}")
    print("Press Ctrl+C to stop\n")

    txn_count = 0
    rapid_fire_account = None
    rapid_fire_count = 0

    while True:
        account_id = random.choice(ACCOUNT_IDS)

        # Every 20 transactions inject a suspicious one
        if txn_count > 0 and txn_count % 20 == 0:
            fraud_type = random.choice(["large_amount", "rapid_fire", "overseas_night"])

            if fraud_type == "rapid_fire":
                rapid_fire_account = account_id
                rapid_fire_count = random.randint(5, 8)

            txn = generate_suspicious_transaction(account_id, fraud_type)
            print(f"🚨 SUSPICIOUS [{fraud_type}] acc={account_id} amount=${txn['amount']}")
        
        elif rapid_fire_count > 0:
            txn = generate_suspicious_transaction(rapid_fire_account, "rapid_fire")
            rapid_fire_count -= 1
            print(f"🚨 RAPID FIRE  acc={rapid_fire_account} amount=${txn['amount']} ({rapid_fire_count} remaining)")
        
        else:
            txn = generate_normal_transaction(account_id)
            print(f"✅ normal      acc={account_id} amount=${txn['amount']} cat={txn['merchant_category']}")

        producer.send(KAFKA_TOPIC, value=txn)
        txn_count += 1

        # Random delay between 0.5 and 1.5 seconds
        time.sleep(random.uniform(0.5, 1.5))

if __name__ == "__main__":
    main()
