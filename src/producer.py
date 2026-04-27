"""
Kafka Producer — Soccer Match Events
=====================================
Reads the prepared match dataset (Parquet) and sends each row as a JSON
message to the Kafka topic 'football-matches'.

A small pause between messages simulates a continuous event stream.

Usage:
    python src/producer.py

Environment:
    KAFKA_BOOTSTRAP_SERVERS  – default: localhost:9092
    KAFKA_TOPIC              – default: football-matches
    MESSAGE_DELAY            – seconds between messages (default: 0.3)
"""

import json
import os
import sys
import time

import pandas as pd
from confluent_kafka import Producer

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
KAFKA_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "football-matches")
MESSAGE_DELAY = float(os.getenv("MESSAGE_DELAY", "0.3"))
PARQUET_PATH = "outputs/exported_dataset"

# ---------------------------------------------------------------------------
# Delivery callback
# ---------------------------------------------------------------------------
_delivery_errors = 0


def delivery_callback(err, msg):
    """Called once per message to indicate delivery result."""
    global _delivery_errors
    if err is not None:
        _delivery_errors += 1
        print(f"  ✗ Delivery failed: {err}", file=sys.stderr)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    global _delivery_errors

    # --- Load the Parquet dataset with pandas (lightweight, no Spark needed) ---
    print(f"Loading dataset from {PARQUET_PATH} …")
    df = pd.read_parquet(PARQUET_PATH)
    print(f"  → {len(df)} rows loaded.\n")

    # --- Create Kafka producer ---
    producer = Producer({
        "bootstrap.servers": KAFKA_SERVERS,
        "client.id": "football-producer",
        "acks": "all",
    })

    print(f"Sending messages to topic '{TOPIC}' on {KAFKA_SERVERS}")
    print(f"Delay between messages: {MESSAGE_DELAY}s\n")

    sent = 0
    for idx, row in df.iterrows():
        # Build the message payload
        message = {
            "season": str(row.get("season", "")),
            "date": str(row.get("date", "")),
            "home_team": str(row.get("home_team", "")),
            "away_team": str(row.get("away_team", "")),
            "home_goals": int(row.get("home_goals", 0)),
            "away_goals": int(row.get("away_goals", 0)),
            "total_goals": int(row.get("total_goals", 0)),
            "goal_difference": int(row.get("goal_difference", 0)),
            "result": str(row.get("result", "")),
            "match_year": int(row.get("match_year", 0)),
        }

        value = json.dumps(message, ensure_ascii=False)

        # Use home_team as the partition key for ordering per team
        key = message["home_team"]

        producer.produce(
            topic=TOPIC,
            key=key.encode("utf-8"),
            value=value.encode("utf-8"),
            callback=delivery_callback,
        )

        sent += 1

        # Trigger any callbacks from previous produces
        producer.poll(0)

        # Progress reporting
        if sent % 50 == 0:
            print(f"  ► Sent {sent}/{len(df)} messages …")

        # Simulate real-time stream
        time.sleep(MESSAGE_DELAY)

    # Wait for all outstanding messages to be delivered
    print("\nFlushing remaining messages …")
    producer.flush(timeout=30)

    print(f"\n{'=' * 50}")
    print(f"  Done!  {sent} messages sent, {_delivery_errors} errors.")
    print(f"{'=' * 50}")


if __name__ == "__main__":
    main()
