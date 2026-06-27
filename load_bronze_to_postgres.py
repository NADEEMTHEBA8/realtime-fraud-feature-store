"""
Load bronze Parquet (MinIO) into Postgres as bronze.transactions.

Bridges the streaming output into the warehouse so dbt can read it as a
source. All columns land as TEXT — typing happens in stg_transactions.
Re-running fully replaces the table.
"""

import psycopg2
from psycopg2.extras import execute_values

from streaming.spark.src.config import create_spark_session


def main() -> None:
    spark = create_spark_session("bronze-loader")
    df = spark.read.format("delta").load("s3a://bronze/transactions_v2/")

    # _raw_json is kept in bronze Parquet for debugging but is not needed
    # (and is bulky) in the warehouse table.
    columns = [c for c in df.columns if c != "_raw_json"]
    rows = [tuple(str(v) if v is not None else None for v in r)
            for r in df.select(columns).collect()]
    spark.stop()
    print(f"Read {len(rows)} rows from MinIO")

    conn = psycopg2.connect(
        host="127.0.0.1",
        port=5434,
        dbname="fraud_reference",
        user="fraud_admin",
        password="changeme_local_only",
    )
    try:
        with conn, conn.cursor() as cur:
            cur.execute("CREATE SCHEMA IF NOT EXISTS bronze")
            cur.execute("DROP TABLE IF EXISTS bronze.transactions CASCADE")
            col_defs = ", ".join(f'"{c}" TEXT' for c in columns)
            cur.execute(f"CREATE TABLE bronze.transactions ({col_defs})")

            col_list = ", ".join(f'"{c}"' for c in columns)
            execute_values(
                cur,
                f"INSERT INTO bronze.transactions ({col_list}) VALUES %s",
                rows,
            )
        print(f"Loaded {len(rows)} rows into bronze.transactions")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
