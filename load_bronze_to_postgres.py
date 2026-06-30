"""
Load bronze Parquet (MinIO) into Postgres as bronze.transactions.

Bridges the streaming output into the warehouse so dbt can read it as a
source. All columns land as TEXT — typing happens in stg_transactions.
Uses Spark's native JDBC writer to prevent OOM errors, and performs a
transactional staging table swap to ensure zero downtime.
"""

import os

import psycopg2
from pyspark.sql.functions import col

from streaming.spark.src.config import create_spark_session


def main() -> None:
    spark = create_spark_session("bronze-loader")
    df = spark.read.format("delta").load("s3a://bronze/transactions_v2/")

    # _raw_json is kept in bronze Parquet for debugging but is not needed
    # (and is bulky) in the warehouse table.
    df = df.drop("_raw_json")

    # Cast all columns to string to match Postgres TEXT expectations for raw layer
    for c in df.columns:
        df = df.withColumn(c, col(c).cast("string"))

    pg_url = "jdbc:postgresql://127.0.0.1:5434/fraud_reference"
    pg_user = os.getenv("PG_USER", "fraud_admin")
    pg_password = os.getenv("PG_PASSWORD", "changeme_local_only")

    properties = {"user": pg_user, "password": pg_password, "driver": "org.postgresql.Driver"}

    print("Writing data to temporary staging table via JDBC...")
    # Write to a temporary staging table
    staging_table = "bronze.transactions_staging"
    target_table = "bronze.transactions"

    # Ensure schema exists using psycopg2 before spark writes
    conn = psycopg2.connect(
        host="127.0.0.1",
        port=5434,
        dbname="fraud_reference",
        user=pg_user,
        password=pg_password,
    )
    try:
        with conn, conn.cursor() as cur:
            cur.execute("CREATE SCHEMA IF NOT EXISTS bronze")
    finally:
        conn.close()

    df.write.jdbc(url=pg_url, table=staging_table, mode="overwrite", properties=properties)

    print("Performing transactional table swap...")
    # Perform transactional swap
    conn = psycopg2.connect(
        host="127.0.0.1",
        port=5434,
        dbname="fraud_reference",
        user=pg_user,
        password=pg_password,
    )
    try:
        with conn, conn.cursor() as cur:
            cur.execute("BEGIN;")
            cur.execute(f"DROP TABLE IF EXISTS {target_table} CASCADE;")
            cur.execute(f"ALTER TABLE {staging_table} RENAME TO transactions;")
            cur.execute("COMMIT;")
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        conn.close()

    print(f"Successfully loaded data into {target_table}")
    spark.stop()


if __name__ == "__main__":
    main()
