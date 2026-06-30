"""
Transaction generator runner.
Publishes synthetic transaction events to Kafka.
"""

from __future__ import annotations

import argparse
import time

from ingestion.transaction_generator.src.generator import TransactionGenerator
from ingestion.transaction_generator.src.kafka_producer import TransactionKafkaProducer
from ingestion.transaction_generator.src.profiles import ProfileFactory
from ingestion.transaction_generator.src.seed_reference import (
    NUM_MERCHANTS,
    NUM_USERS,
    SEED,
)

KAFKA_BOOTSTRAP = "localhost:9092"
KAFKA_TOPIC = "transactions.raw"
PRINT_EVERY = 50


def main() -> None:
    parser = argparse.ArgumentParser(description="Transaction generator runner.")
    parser.add_argument(
        "--rate", type=int, default=10, help="Target events per second (default: 10)"
    )
    parser.add_argument(
        "--firehose", action="store_true", help="Aggressively batch events without sleep"
    )
    parser.add_argument("--max-events", type=int, default=0, help="Stop after this many events")
    args = parser.parse_args()

    print(f"Generating {NUM_USERS} users / {NUM_MERCHANTS} merchants (seed={SEED})")
    factory = ProfileFactory(seed=SEED)
    users = factory.make_users(NUM_USERS)
    merchants = factory.make_merchants(NUM_MERCHANTS)

    gen = TransactionGenerator(users=users, merchants=merchants, seed=SEED)
    producer = TransactionKafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        topic=KAFKA_TOPIC,
    )

    if args.firehose:
        print(f"Publishing to {KAFKA_TOPIC} in FIREHOSE mode (Ctrl+C to stop)")
    else:
        print(f"Publishing to {KAFKA_TOPIC} at ~{args.rate}/s (Ctrl+C to stop)")

    delay = 1.0 / args.rate if args.rate > 0 else 0
    total_sent = 0
    start_time = time.time()

    try:
        while True:
            event = gen.generate_one()
            producer.send(event)
            total_sent += 1

            if args.max_events > 0 and total_sent >= args.max_events:
                break

            if total_sent % PRINT_EVERY == 0:
                elapsed = time.time() - start_time
                actual_rate = total_sent / elapsed if elapsed > 0 else 0
                print(f"  sent={total_sent:>6}  rate={actual_rate:.1f}/s")
                producer.flush()

            if not args.firehose and delay > 0:
                time.sleep(delay)

    except KeyboardInterrupt:
        pass

    finally:
        producer.flush()
        elapsed = time.time() - start_time
        stats = producer.stats
        actual_rate = stats["sent"] / elapsed if elapsed > 0 else 0
        print(
            f"\nStopped. sent={stats['sent']} errors={stats['errors']} "
            f"duration={elapsed:.1f}s avg_rate={actual_rate:.1f}/s"
        )
        producer.close()


if __name__ == "__main__":
    main()
