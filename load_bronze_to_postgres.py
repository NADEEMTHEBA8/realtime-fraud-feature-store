"""
Load bronze Parquet data from MinIO into Postgres.

This is a one-time data loading script for local dev.
In production, you'd use an external table or ELT tool.
"""

import psycopg2
from streaming.spark.src.config import create_spark_session


def main():
    # Step 1: Read parquet from MinIO using Spark
    print("Reading parquet from MinIO...")
    spark = create_spark_session("loader")
    df = spark.read.parquet("s3a://bronze/transactions/")

    # Drop _raw_json (too large for Postgres TEXT inserts) and convert to list of dicts
    columns = [c for c in df.columns if c != "_raw_json"]
    rows = df.select(columns).collect()
    total = len(rows)
    print(f"Read {total} rows from MinIO")
    spark.stop()

    # Step 2: Write to Postgres
    print("Connecting to Postgres...")
    conn = psycopg2.connect(
        host="127.0.0.1",
        port=5432,
        dbname="fraud_reference",
        user="fraud_admin",
        password="changeme_local_only",
    )
    cur = conn.cursor()

    # Create bronze schema and table
    cur.execute("CREATE SCHEMA IF NOT EXISTS bronze")
    cur.execute("DROP TABLE IF EXISTS bronze.transactions")

    # All columns as TEXT for bronze layer (raw, untyped)
    col_defs = ", ".join([f'"{c}" TEXT' for c in columns])
    cur.execute(f"CREATE TABLE bronze.transactions ({col_defs})")

    # Batch insert
    placeholders = ",".join(["%s"] * len(columns))
    inserted = 0
    for row in rows:
        vals = tuple(
            str(v) if v is not None else None for v in row
        )
        cur.execute(
            f"INSERT INTO bronze.transactions VALUES ({placeholders})", vals
        )
        inserted += 1
        if inserted % 500 == 0:
            print(f"  Inserted {inserted}/{total}...")

    conn.commit()
    cur.close()
    conn.close()
    print(f"Done! Loaded {inserted} rows into bronze.transactions")


if __name__ == "__main__":
    main()
