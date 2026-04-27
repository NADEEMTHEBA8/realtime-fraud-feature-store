"""
Transaction generator runner.

This is the entry point that ties everything together:
1. Creates user and merchant profiles
2. Initializes the transaction generator
3. Connects to Kafka
4. Produces events in a continuous loop at a configurable rate
5. Prints progress every N events

Usage:
    python -m ingestion.transaction_generator.src.run

Press Ctrl+C to stop gracefully.
"""

from __future__ import annotations

import sys
import time

from ingestion.transaction_generator.src.generator import TransactionGenerator
from ingestion.transaction_generator.src.kafka_producer import TransactionKafkaProducer
from ingestion.transaction_generator.src.profiles import ProfileFactory


def main() -> None:
    # ----- Configuration -----
    NUM_USERS = 5000
    NUM_MERCHANTS = 500
    EVENTS_PER_SECOND = 10       # Start slow; increase once verified
    SEED = 42                     # Reproducible profiles
    KAFKA_BOOTSTRAP = "localhost:9092"
    KAFKA_TOPIC = "transactions.raw"
    PRINT_EVERY = 50              # Print progress every N events

    print("=" * 60)
    print("  TRANSACTION GENERATOR")
    print("=" * 60)

    # ----- Step 1: Generate profiles -----
    print(f"\n[1/3] Generating {NUM_USERS} users and {NUM_MERCHANTS} merchants...")
    factory = ProfileFactory(seed=SEED)
    users = factory.make_users(NUM_USERS)
    merchants = factory.make_merchants(NUM_MERCHANTS)
    print(f"      Done. Users from {len(set(u.city for u in users))} cities.")

    # ----- Step 2: Initialize generator -----
    print(f"[2/3] Initializing transaction generator...")
    gen = TransactionGenerator(users=users, merchants=merchants, seed=SEED)
    print(f"      Target rate: {EVENTS_PER_SECOND} events/sec")

    # ----- Step 3: Connect to Kafka -----
    print(f"[3/3] Connecting to Kafka at {KAFKA_BOOTSTRAP}...")
    producer = TransactionKafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        topic=KAFKA_TOPIC,
    )
    print(f"      Publishing to topic: {KAFKA_TOPIC}")

    # ----- Run loop -----
    print(f"\n{'=' * 60}")
    print(f"  PRODUCING EVENTS (Ctrl+C to stop)")
    print(f"{'=' * 60}\n")

    delay = 1.0 / EVENTS_PER_SECOND
    total_sent = 0
    start_time = time.time()

    try:
        while True:
            event = gen.generate_one()
            producer.send(event)
            total_sent += 1

            if total_sent % PRINT_EVERY == 0:
                elapsed = time.time() - start_time
                rate = total_sent / elapsed if elapsed > 0 else 0
                print(
                    f"  Sent {total_sent:>6} events | "
                    f"Rate: {rate:.1f}/sec | "
                    f"Last: {event.user_id} -> {event.merchant_id} "
                    f"INR {event.amount} ({event.payment_method.value})"
                )
                producer.flush()  # Flush periodically

            time.sleep(delay)

    except KeyboardInterrupt:
        print(f"\n\nShutting down gracefully...")
        producer.flush()
        elapsed = time.time() - start_time
        stats = producer.stats
        print(f"\n{'=' * 60}")
        print(f"  SUMMARY")
        print(f"{'=' * 60}")
        print(f"  Total events sent:  {stats['sent']}")
        print(f"  Errors:             {stats['errors']}")
        print(f"  Duration:           {elapsed:.1f} seconds")
        print(f"  Avg rate:           {stats['sent'] / elapsed:.1f} events/sec")
        print(f"  Topic:              {stats['topic']}")
        print(f"{'=' * 60}\n")
        producer.close()


if __name__ == "__main__":
    main()
