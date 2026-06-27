"""
Bronze ingestion: Kafka transactions.raw to Delta Lake.
"""

import argparse
import logging
import os

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, current_timestamp, from_json, hour, sha2, to_date
from pyspark.sql.types import StringType, StructField, StructType, TimestampType

from streaming.spark.src.config import create_spark_session

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("bronze_ingest")

# Mirrors schemas.TransactionEvent. Optional fields are nullable; everything
# stays StringType so a malformed value never fails the whole micro-batch.
TRANSACTION_SCHEMA = StructType([
    StructField("transaction_id", StringType(), nullable=False),
    StructField("user_id", StringType(), nullable=False),
    StructField("merchant_id", StringType(), nullable=False),
    StructField("amount", StringType(), nullable=False),
    StructField("currency", StringType(), nullable=False),
    StructField("transaction_type", StringType(), nullable=False),
    StructField("status", StringType(), nullable=False),
    StructField("payment_method", StringType(), nullable=False),
    StructField("event_timestamp", StringType(), nullable=False),
    StructField("ingestion_timestamp", StringType(), nullable=True),
    StructField("device_id", StringType(), nullable=True),
    StructField("ip_address", StringType(), nullable=True),
    StructField("city", StringType(), nullable=True),
    StructField("country", StringType(), nullable=True),
])


def build_kafka_source(spark: SparkSession, bootstrap: str, topic: str) -> DataFrame:
    """Streaming DataFrame over a Kafka topic, reading from earliest offsets.

    The checkpoint tracks consumed offsets across restarts.
    maxOffsetsPerTrigger caps batch size on the first (full backlog) run.
    """
    logger.info("Kafka source: %s topic=%s", bootstrap, topic)
    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", bootstrap)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .option("maxOffsetsPerTrigger", 10000)
        .load()
    )


def parse_and_validate(raw_df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Parse Kafka values as JSON, split into (valid, invalid).

    A record is invalid when JSON parsing fails — required fields come back
    null. Invalid records keep the original payload as _raw_json so the
    dead-letter consumer can see what was malformed.
    """
    parsed = (
        raw_df
        .withColumn("value_str", col("value").cast("string"))
        .withColumn("parsed", from_json(col("value_str"), TRANSACTION_SCHEMA))
        .select(
            col("parsed.*"),
            col("topic").alias("_kafka_topic"),
            col("partition").alias("_kafka_partition"),
            col("offset").alias("_kafka_offset"),
            col("timestamp").alias("_kafka_timestamp"),
            current_timestamp().alias("_processing_timestamp"),
            col("value_str").alias("_raw_json"),
        )
    )

    required = (
        col("transaction_id").isNotNull()
        & col("user_id").isNotNull()
        & col("amount").isNotNull()
    )
    return parsed.filter(required), parsed.filter(~required)


def add_partition_columns(df: DataFrame) -> DataFrame:
    """Derive event_date / event_hour from event_timestamp for Parquet layout."""
    return (
        df
        .withColumn("event_ts_parsed", col("event_timestamp").cast(TimestampType()))
        .withColumn("event_date", to_date(col("event_ts_parsed")))
        .withColumn("event_hour", hour(col("event_ts_parsed")))
    )


def mask_pii(df: DataFrame) -> DataFrame:
    """Mask PII fields using SHA-256 for data governance compliance."""
    return (
        df
        .withColumn("device_id", sha2(col("device_id"), 256))
        .withColumn("ip_address", sha2(col("ip_address"), 256))
    )


def write_bronze_to_minio(df: DataFrame, checkpoint: str, output: str, once: bool = False):
    """Append valid records as date/hour-partitioned Delta Lake tables on MinIO."""
    logger.info("Bronze sink: %s", output)
    writer = df.writeStream.format("delta").outputMode("append")
    
    if once:
        writer = writer.trigger(availableNow=True)
    else:
        writer = writer.trigger(processingTime="30 seconds")

    return (
        writer
        # TODO: S3 Small File Problem - Triggering micro-batches every 30 seconds creates 
        # thousands of tiny Parquet files per hour partition under low volume. This will 
        # severely degrade downstream dbt load performance. We need to implement a compaction 
        # job or dynamically tune the trigger interval.
        .option("checkpointLocation", checkpoint)
        .option("path", output)
        .partitionBy("event_date", "event_hour")
        .start()
    )


def write_dead_letters(df: DataFrame, checkpoint: str, bootstrap: str, once: bool = False):
    """Route unparseable records to the transactions.dead_letter topic."""
    logger.info("Dead-letter sink: transactions.dead_letter")
    writer = (
        df
        .select(
            col("_kafka_offset").cast("string").alias("key"),
            col("_raw_json").alias("value"),
        )
        .writeStream
        .format("kafka")
        .outputMode("append")
    )
    
    if once:
        writer = writer.trigger(availableNow=True)
    else:
        writer = writer.trigger(processingTime="30 seconds")

    return (
        writer
        .option("kafka.bootstrap.servers", bootstrap)
        .option("topic", "transactions.dead_letter")
        .option("checkpointLocation", checkpoint)
        .start()
    )


def run() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--once", action="store_true", help="Process all available data then stop")
    args = parser.parse_args()

    bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    topic = "transactions.raw"
    bronze_output = "s3a://bronze/transactions_v2/"
    bronze_checkpoint = "s3a://bronze/_checkpoints/bronze_ingest_v2/"
    dead_letter_checkpoint = "s3a://bronze/_checkpoints/dead_letter/"

    spark = create_spark_session(app_name="bronze-ingest")
    try:
        raw_df = build_kafka_source(spark, bootstrap, topic)
        valid_df, invalid_df = parse_and_validate(raw_df)
        valid_df = mask_pii(valid_df)
        valid_df = add_partition_columns(valid_df)

        write_bronze_to_minio(valid_df, bronze_checkpoint, bronze_output, once=args.once)
        write_dead_letters(invalid_df, dead_letter_checkpoint, bootstrap, once=args.once)

        logger.info("Streams started.")
        
        if args.once:
            for q in spark.streams.active:
                q.awaitTermination()
        else:
            logger.info("Press Ctrl+C to stop.")
            spark.streams.awaitAnyTermination()
            
    except KeyboardInterrupt:
        logger.info("Shutdown signal received.")
    except Exception:
        logger.exception("Bronze ingestion failed")
        raise
    finally:
        spark.stop()
        logger.info("Spark session stopped.")


if __name__ == "__main__":
    run()
