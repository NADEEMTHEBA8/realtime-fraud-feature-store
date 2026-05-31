"""
Transaction generator runner.

Builds the user/merchant profiles, then publishes synthetic transaction
events to Kafka at a fixed rate until interrupted.

The profiles are regenerated from the same ProfileFactory seed used by
seed_reference.py, so every user_id / merchant_id emitted here exists as a
row in public.users / public.merchants. Seed the reference tables first:
    python -m ingestion.transaction_generator.src.seed_reference

Usage:
    python -m ingestion.transaction_generator.src.run   (Ctrl+C to stop)
"""

from __future__ import annotations

import time

from ingestion.transaction_generator.src.generator import TransactionGenerator
from ingestion.transaction_generator.src.kafka_producer import TransactionKafkaProducer
from ingestion.transaction_generator.src.profiles import ProfileFactory
from ingestion.transaction_generator.src.seed_reference import (
    NUM_MERCHANTS,
    NUM_USERS,
    SEED,
)

EVENTS_PER_SECOND = 10
KAFKA_BOOTSTRAP = "localhost:9092"
KAFKA_TOPIC = "transactions.raw"
PRINT_EVERY = 50


def main() -> None:
    print(f"Generating {NUM_USERS} users / {NUM_MERCHANTS} merchants (seed={SEED})")
    factory = ProfileFactory(seed=SEED)
    users = factory.make_users(NUM_USERS)
    merchants = factory.make_merchants(NUM_MERCHANTS)

    gen = TransactionGenerator(users=users, merchants=merchants, seed=SEED)
    producer = TransactionKafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        topic=KAFKA_TOPIC,
    )
    print(f"Publishing to {KAFKA_TOPIC} at ~{EVENTS_PER_SECOND}/s (Ctrl+C to stop)")

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
                print(f"  sent={total_sent:>6}  rate={rate:.1f}/s")
                producer.flush()

            time.sleep(delay)

    except KeyboardInterrupt:
        producer.flush()
        elapsed = time.time() - start_time
        stats = producer.stats
        rate = stats["sent"] / elapsed if elapsed > 0 else 0
        print(
            f"\nStopped. sent={stats['sent']} errors={stats['errors']} "
            f"duration={elapsed:.1f}s avg_rate={rate:.1f}/s"
        )
        producer.close()


if __name__ == "__main__":
    main()
