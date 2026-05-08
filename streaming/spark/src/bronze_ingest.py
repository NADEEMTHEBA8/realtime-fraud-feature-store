"""
Bronze layer: Kafka → Parquet on MinIO.

What this job does:
    1. Reads raw JSON events from Kafka topic 'transactions.raw'
    2. Parses them against an explicit schema (no schema inference!)
    3. Adds ingestion metadata (kafka partition, offset, processing time)
    4. Routes valid records → Parquet on MinIO (s3a://bronze/transactions/)
    5. Routes malformed records → Kafka dead-letter topic

Why Parquet?
    - Columnar format: reading 3 columns out of 20 only touches those 3
    - Compressed: ~10x smaller than JSON
    - Schema-embedded: the file carries its own schema
    - Every major warehouse (BigQuery, Snowflake, Redshift) reads it natively

Why partition by date + hour?
    - Queries like "all transactions on 2025-01-15 between 2pm-3pm" only scan
      that one partition folder instead of the entire dataset
    - This is called "partition pruning" — massive performance win at scale

Interview talking point:
    "The bronze layer is append-only, schema-on-read. I preserve the raw event
    exactly as Kafka delivered it — including the Kafka metadata (partition, offset)
    so I can replay from any point if the silver layer logic changes."
"""

import os
import json
import logging
from datetime import datetime

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import (
    StructType, StructField, StringType, DecimalType,
    TimestampType, IntegerType
)
from pyspark.sql.functions import (
    col, from_json, to_date, hour, current_timestamp,
    lit, year, month, dayofmonth, when, length
)

from streaming.spark.src.config import create_spark_session


# ---------- Logging ----------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger("bronze_ingest")


# ---------- Schema Definition ----------
# This MUST match schemas.py from Day 3. We define it again in Spark types
# because PySpark can't use Pydantic models directly.
#
# Rule: Bronze schema is PERMISSIVE — we use StringType for enums and
# keep validation light. Heavy validation happens in the Silver layer.

TRANSACTION_SCHEMA = StructType([
    StructField("transaction_id", StringType(), nullable=False),
    StructField("user_id", StringType(), nullable=False),
    StructField("merchant_id", StringType(), nullable=False),
    StructField("amount", StringType(), nullable=False),       # String, not Decimal — we parse it safely later
    StructField("currency", StringType(), nullable=True),
    StructField("transaction_type", StringType(), nullable=True),
    StructField("status", StringType(), nullable=True),
    StructField("payment_method", StringType(), nullable=True),
    StructField("merchant_category", StringType(), nullable=True),
    StructField("merchant_name", StringType(), nullable=True),
    StructField("user_email_hash", StringType(), nullable=True),
    StructField("user_phone_hash", StringType(), nullable=True),
    StructField("user_city", StringType(), nullable=True),
    StructField("user_country", StringType(), nullable=True),
    StructField("ip_address", StringType(), nullable=True),
    StructField("device_type", StringType(), nullable=True),
    StructField("channel", StringType(), nullable=True),
    StructField("event_timestamp", StringType(), nullable=False),  # String — we cast after parsing
    StructField("ingestion_timestamp", StringType(), nullable=True),
    StructField("user_kyc_level", StringType(), nullable=True),
    StructField("user_risk_score", StringType(), nullable=True),
    StructField("merchant_risk_tier", StringType(), nullable=True),
])


def build_kafka_source(spark: SparkSession, kafka_bootstrap: str, topic: str) -> DataFrame:
    """
    Create a streaming DataFrame from a Kafka topic.

    What Kafka gives us per message:
        - key: bytes (our user_id, used for partitioning)
        - value: bytes (the JSON event payload)
        - topic: string
        - partition: int (which Kafka partition)
        - offset: long (message sequence number within partition)
        - timestamp: long (Kafka broker timestamp)

    We care about 'value' (the event) and the metadata (partition, offset)
    for lineage tracking and replay capability.
    """
    logger.info(f"Connecting to Kafka at {kafka_bootstrap}, topic: {topic}")

    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", kafka_bootstrap)
        .option("subscribe", topic)
        # Start from earliest: on first run, read all existing messages.
        # On restart, Spark's checkpoint remembers where it left off.
        .option("startingOffsets", "earliest")
        # If Kafka is temporarily down, wait up to 30s before failing
        .option("failOnDataLoss", "false")
        # Max records per trigger — prevents OOM on first run if topic has millions
        .option("maxOffsetsPerTrigger", 10000)
        .load()
    )


def parse_and_validate(raw_df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """
    Parse JSON from Kafka value bytes and split into valid/invalid records.

    Returns:
        (valid_df, invalid_df) — valid records have all required fields non-null;
        invalid records are ones where JSON parsing failed (corrupt/incomplete).

    Why split here?
        In production, you NEVER drop bad records silently. You route them to a
        dead-letter queue so an engineer can investigate. This is a regulatory
        requirement in fintech — every transaction must be accounted for.
    """
    # Step 1: Cast Kafka 'value' from bytes to string, then parse JSON
    parsed_df = (
        raw_df
        # Kafka value is raw bytes — cast to UTF-8 string first
        .withColumn("value_str", col("value").cast("string"))
        # Parse the JSON string against our schema
        # PERMISSIVE mode: if parsing fails, all fields become null (row not dropped)
        .withColumn("parsed", from_json(col("value_str"), TRANSACTION_SCHEMA))
        # Carry Kafka metadata forward for lineage
        .select(
            col("parsed.*"),                           # All parsed event fields
            col("topic").alias("_kafka_topic"),         # Which topic
            col("partition").alias("_kafka_partition"), # Which partition
            col("offset").alias("_kafka_offset"),       # Message offset
            col("timestamp").alias("_kafka_timestamp"), # Kafka broker timestamp
            current_timestamp().alias("_processing_timestamp"),  # When Spark processed it
            col("value_str").alias("_raw_json"),        # Keep raw JSON for debugging
        )
    )

    # Step 2: Split valid vs invalid
    # A record is invalid if JSON parsing failed (transaction_id would be null)
    valid_df = parsed_df.filter(
        col("transaction_id").isNotNull()
        & col("user_id").isNotNull()
        & col("amount").isNotNull()
    )

    invalid_df = parsed_df.filter(
        col("transaction_id").isNull()
        | col("user_id").isNull()
        | col("amount").isNull()
    )

    return valid_df, invalid_df


def add_partition_columns(df: DataFrame) -> DataFrame:
    """
    Add date and hour columns for Parquet partitioning.

    The event_timestamp comes in as a string like "2025-01-15T14:30:00".
    We extract event_date and event_hour so Parquet files are organised as:
        s3a://bronze/transactions/event_date=2025-01-15/event_hour=14/part-00000.parquet

    Why not partition by user_id?
        User-level partitioning creates millions of tiny files (one per user per batch).
        Date+hour gives ~24 partitions per day — manageable and aligns with time-based queries.
    """
    return (
        df
        .withColumn("event_ts_parsed", col("event_timestamp").cast(TimestampType()))
        .withColumn("event_date", to_date(col("event_ts_parsed")))
        .withColumn("event_hour", hour(col("event_ts_parsed")))
    )


def write_bronze_to_minio(valid_df: DataFrame, checkpoint_path: str, output_path: str):
    """
    Write valid records as partitioned Parquet to MinIO bronze bucket.

    Key settings:
        - trigger(processingTime="30 seconds"): Micro-batch every 30s.
          Not true real-time, but good enough for bronze layer and gentle on resources.
        - checkpointLocation: Spark saves its progress here. On restart, it picks up
          exactly where it left off — no duplicates, no gaps. This is how Spark
          achieves exactly-once semantics with Kafka.
        - partitionBy: Physical folder structure on disk for partition pruning.

    In production you'd tune the trigger interval based on latency requirements.
    """
    logger.info(f"Writing bronze parquet to {output_path}")

    return (
        valid_df.writeStream
        .format("parquet")
        .outputMode("append")
        .trigger(processingTime="30 seconds")
        .option("checkpointLocation", checkpoint_path)
        .option("path", output_path)
        .partitionBy("event_date", "event_hour")
        .start()
    )


def write_dead_letters(invalid_df: DataFrame, checkpoint_path: str, kafka_bootstrap: str):
    """
    Route malformed records to a Kafka dead-letter topic.

    Why a dead-letter topic?
        - Bad records need investigation, not silent deletion
        - Downstream consumers of 'transactions.raw' aren't affected
        - An alert can fire when dead-letter count exceeds threshold
        - In fintech audits, you must prove no transaction was lost

    The _raw_json column contains the original unparseable message,
    so engineers can see exactly what was malformed.
    """
    logger.info("Setting up dead-letter stream for invalid records")

    return (
        invalid_df
        .select(
            col("_kafka_offset").cast("string").alias("key"),
            col("_raw_json").alias("value")
        )
        .writeStream
        .format("kafka")
        .outputMode("append")
        .trigger(processingTime="30 seconds")
        .option("kafka.bootstrap.servers", kafka_bootstrap)
        .option("topic", "transactions.dead_letter")
        .option("checkpointLocation", checkpoint_path)
        .start()
    )


def run():
    """
    Main entry point for the bronze ingestion job.

    This runs indefinitely (like any streaming job) until you press Ctrl+C.
    In production, this would run on a Spark cluster managed by YARN or K8s.
    """
    # ---------- Configuration ----------
    kafka_bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    kafka_topic = "transactions.raw"

    # MinIO paths (s3a:// protocol uses the S3A connector we configured)
    bronze_output = "s3a://bronze/transactions/"
    bronze_checkpoint = "s3a://bronze/_checkpoints/bronze_ingest/"
    dead_letter_checkpoint = "s3a://bronze/_checkpoints/dead_letter/"

    logger.info("=" * 60)
    logger.info("BRONZE INGESTION JOB STARTING")
    logger.info(f"  Kafka: {kafka_bootstrap}")
    logger.info(f"  Topic: {kafka_topic}")
    logger.info(f"  Output: {bronze_output}")
    logger.info("=" * 60)

    # ---------- Create Spark Session ----------
    spark = create_spark_session(app_name="bronze-ingest")

    try:
        # ---------- Read from Kafka ----------
        raw_df = build_kafka_source(spark, kafka_bootstrap, kafka_topic)

        # ---------- Parse and Validate ----------
        valid_df, invalid_df = parse_and_validate(raw_df)

        # ---------- Add Partition Columns ----------
        valid_df = add_partition_columns(valid_df)

        # ---------- Write Valid → MinIO Bronze ----------
        bronze_query = write_bronze_to_minio(
            valid_df,
            checkpoint_path=bronze_checkpoint,
            output_path=bronze_output
        )

        # ---------- Write Invalid → Dead Letter Topic ----------
        dead_letter_query = write_dead_letters(
            invalid_df,
            checkpoint_path=dead_letter_checkpoint,
            kafka_bootstrap=kafka_bootstrap
        )

        logger.info("Both streams started. Waiting for data...")
        logger.info("Press Ctrl+C to stop gracefully.")

        # Block until either stream terminates (or Ctrl+C)
        spark.streams.awaitAnyTermination()

    except KeyboardInterrupt:
        logger.info("Received shutdown signal. Stopping streams...")
    except Exception as e:
        logger.error(f"Bronze ingestion failed: {e}", exc_info=True)
        raise
    finally:
        spark.stop()
        logger.info("Spark session stopped. Bronze ingestion complete.")


if __name__ == "__main__":
    run()
