"""
Mock ML Scoring Service.

Simulates the real-time decision engine (the "Bouncer") that sits in front of the
Feature Store API. It intercepts a dummy transaction, queries the API for the
user's historical features, and runs a mock ML heuristic to ALLOW or BLOCK the transaction.
"""

import argparse
import random
import sys
import time

import psycopg2
import requests

API_URL = "http://localhost:8002/v1/features/user/{}"
API_KEY = "sk_test_123"


def get_random_user_from_db():
    """Simulate getting a user ID from an active checkout session."""
    print("INFO: Connecting to DB to find an active user...")
    try:
        conn = psycopg2.connect(
            host="localhost",
            port=5434,
            database="fraud_reference",
            user="fraud_admin",
            password="changeme_local_only",
        )
        cur = conn.cursor()
        cur.execute(
            "SELECT user_id FROM silver_gold.gold_user_fraud_features ORDER BY random() LIMIT 1;"
        )
        res = cur.fetchone()
        conn.close()

        if res:
            return res[0]
        else:
            print("ERROR: No users found. Run `make demo` first.")
            sys.exit(1)
    except Exception as e:
        print(f"ERROR: Failed to connect to Postgres: {e}")
        print("Ensure docker containers are running.")
        sys.exit(1)


def score_transaction(user_id: str):
    print(f"\nINFO: User {user_id} is attempting a purchase.")
    print("INFO: Pinging Feature Store API for user history...")

    headers = {"X-API-Key": API_KEY}

    # 1. Fetch Features
    start_time = time.perf_counter()
    try:
        response = requests.get(API_URL.format(user_id), headers=headers, timeout=2.0)
    except requests.exceptions.ConnectionError:
        print("ERROR: Failed to connect to API.")
        sys.exit(1)

    network_latency_ms = (time.perf_counter() - start_time) * 1000

    if response.status_code != 200:
        print(f"ERROR: API returned {response.status_code}")
        sys.exit(1)

    # 2. Extract Data
    data = response.json()
    server_process_time = response.headers.get("X-Process-Time-Ms", "Unknown")
    features = data["features"]

    print("INFO: Features retrieved successfully.")
    print(f"INFO: API Processing Time: {server_process_time} ms")
    print(f"INFO: Total Latency: {network_latency_ms:.2f} ms\n")

    # 3. Mock ML Decision Engine
    print("--- ML SCORING START ---")

    # Extract specific features the model cares about
    txn_count_24h = features.get("txn_count_24h", 0)
    failure_rate_24h = features.get("failure_rate_24h", 0.0)
    unique_cities_24h = features.get("unique_cities_24h", 1)
    late_night_txn_count = features.get("late_night_txn_count_24h", 0)
    zscore = features.get("latest_amount_zscore", 0.0)

    print("Extracted features:")
    print(f" - Transactions (24h): {txn_count_24h}")
    print(f" - Failure Rate (24h): {failure_rate_24h}")
    print(f" - Unique Cities (24h): {unique_cities_24h}")
    print(f" - Late Night Swipes: {late_night_txn_count}")
    print(f" - Amount Z-Score: {zscore}\n")

    # Calculate Risk Score (0-100)
    risk_score = 0
    reasons = []

    if txn_count_24h > 15:
        risk_score += 30
        reasons.append("High velocity (card testing).")
    if failure_rate_24h > 0.4:
        risk_score += 40
        reasons.append("Extreme failure rate.")
    if unique_cities_24h > 2:
        risk_score += 30
        reasons.append("Impossible travel speed (swiped in multiple cities).")
    if late_night_txn_count > 2:
        risk_score += 20
        reasons.append("Suspicious late night activity.")
    if zscore > 3.0:
        risk_score += 25
        reasons.append("Purchase amount deviates severely from user norm.")

    # Add random baseline noise (0-15) to simulate typical ML score variance
    risk_score += random.randint(0, 15)
    risk_score = min(risk_score, 100)

    print(f"Calculated Risk Score: {risk_score}/100")

    # 4. Enforce Policy
    if risk_score > 75:
        print("ACTION: BLOCKED")
        print("Reasons:")
        for r in reasons:
            print(f" - {r}")
    else:
        print("ACTION: APPROVED")
        print("Transaction allowed.")

    print("\n------------------------------------------------")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--user_id", type=str, help="Specific user_id to test")
    args = parser.parse_args()

    user = args.user_id if args.user_id else get_random_user_from_db()
    score_transaction(user)
