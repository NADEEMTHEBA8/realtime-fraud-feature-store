"""
SparkSession factory for the bronze ingestion job (Kafka source, MinIO S3A sink).

Connector JARs are resolved from Maven at submit time via spark.jars.packages.
Pinned to Spark 3.5.x / Hadoop 3.3.4 (the Hadoop version Spark 3.5 bundles);
Ivy pulls the transitive deps (kafka-clients, aws-java-sdk-bundle, etc.).
"""

import os

from pyspark.sql import SparkSession

SPARK_PACKAGES = ",".join([
    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0",
    "org.apache.hadoop:hadoop-aws:3.3.4",
])


def create_spark_session(app_name: str = "fraud-feature-store") -> SparkSession:
    """Local SparkSession wired for the Kafka source and the MinIO S3A sink.

    MinIO credentials default to the docker-compose defaults; override via the
    same MINIO_* variables used by docker-compose (see .env.example).
    """
    minio_endpoint = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
    minio_user = os.getenv("MINIO_ROOT_USER", "minioadmin")
    minio_password = os.getenv("MINIO_ROOT_PASSWORD", "minioadmin")

    spark = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.driver.memory", "2g")
        .config("spark.jars.packages", SPARK_PACKAGES)

        # S3A -> MinIO. Path-style access because MinIO does not serve the
        # virtual-host bucket addressing that real S3 uses.
        .config("spark.hadoop.fs.s3a.endpoint", minio_endpoint)
        .config("spark.hadoop.fs.s3a.access.key", minio_user)
        .config("spark.hadoop.fs.s3a.secret.key", minio_password)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")

        # Streaming jobs always use an explicit schema; never infer.
        .config("spark.sql.streaming.schemaInference", "false")
        .config("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MICROS")

        .master("local[2]")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark
