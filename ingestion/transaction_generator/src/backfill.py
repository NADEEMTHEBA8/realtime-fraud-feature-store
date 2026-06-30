"""
Historical backfill generator.
Writes synthetic transaction events to partitioned Parquet files.
"""

import argparse
import time

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from ingestion.transaction_generator.src.generator import TransactionGenerator
from ingestion.transaction_generator.src.profiles import ProfileFactory
from ingestion.transaction_generator.src.seed_reference import (
    NUM_MERCHANTS,
    NUM_USERS,
    SEED,
)


def generate_and_write_chunk(gen: TransactionGenerator, chunk_size: int, output_path: str) -> None:
    events = gen.generate_batch(chunk_size)

    records = []
    for e in events:
        record = e.model_dump(mode="json")
        record["event_date"] = e.event_timestamp.strftime("%Y-%m-%d")
        record["event_hour"] = e.event_timestamp.strftime("%H")
        records.append(record)

    df = pd.DataFrame(records)
    table = pa.Table.from_pandas(df)

    pq.write_to_dataset(
        table,
        root_path=output_path,
        partition_cols=["event_date", "event_hour"],
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Historical backfill generator.")
    parser.add_argument("--rows", type=int, default=1_000_000, help="Number of rows to generate")
    parser.add_argument(
        "--chunk-size", type=int, default=100_000, help="Chunk size for memory management"
    )
    parser.add_argument("--output", type=str, default="bronze/transactions/", help="Output path")
    args = parser.parse_args()

    print(f"Initializing generator with {NUM_USERS} users and {NUM_MERCHANTS} merchants...")
    factory = ProfileFactory(seed=SEED)
    users = factory.make_users(NUM_USERS)
    merchants = factory.make_merchants(NUM_MERCHANTS)
    gen = TransactionGenerator(users=users, merchants=merchants, seed=SEED)

    print(f"Starting backfill of {args.rows} rows to {args.output}")
    start_time = time.time()

    rows_generated = 0
    while rows_generated < args.rows:
        chunk = min(args.chunk_size, args.rows - rows_generated)
        generate_and_write_chunk(gen, chunk, args.output)
        rows_generated += chunk

        elapsed = time.time() - start_time
        print(f"  Generated {rows_generated:,} rows... ({(rows_generated / elapsed):.0f} rows/s)")

    print(f"Backfill complete in {time.time() - start_time:.1f}s")


if __name__ == "__main__":
    main()
