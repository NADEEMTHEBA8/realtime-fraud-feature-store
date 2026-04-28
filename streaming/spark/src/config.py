"""
Spark session factory with Kafka + MinIO (S3A) configuration.

Why this file exists:
    Every Spark job needs a SparkSession. Rather than repeat config in every job,
    we centralise it here. This is the pattern used in production Spark codebases.

Key configs explained:
    - spark.jars: Points to the 6 JARs we downloaded (Kafka + S3A connectors)
    - spark.hadoop.fs.s3a.*: Tells Spark to treat "s3a://" paths as MinIO, not real AWS S3
    - spark.sql.streaming.schemaInference: Disabled — we always define schemas explicitly
      (production rule: never let Spark guess your schema)
"""

import os
from pathlib import Path
from pyspark.sql import SparkSession


def get_jar_paths() -> str:
    """
    Build comma-separated string of all JAR paths in streaming/spark/jars/.

    Why not use --packages flag?
        --packages downloads from Maven at runtime. That's flaky in CI and
        behind corporate proxies. Pre-downloaded JARs are deterministic.
    """
    jar_dir = Path(__file__).parent.parent / "jars"
    jars = list(jar_dir.glob("*.jar"))

    if not jars:
        raise FileNotFoundError(
            f"No JARs found in {jar_dir}. "
            f"Run the curl commands from Day 4 Step 2 to download them."
        )

    return ",".join(str(j) for j in jars)


def create_spark_session(app_name: str = "fraud-feature-store") -> SparkSession:
    """
    Create a SparkSession configured for local dev with Kafka + MinIO.

    Args:
        app_name: Shows up in Spark UI (localhost:4040 while job runs).

    Returns:
        Configured SparkSession.
    """
    # Read MinIO credentials from environment (same .env as docker-compose)
    minio_endpoint = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
    minio_access_key = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
    minio_secret_key = os.getenv("MINIO_SECRET_KEY", "minioadmin")

    jar_paths = get_jar_paths()

    spark = (
        SparkSession.builder
        .appName(app_name)

        # ---------- Resource limits ----------
        # Your MacBook has 16GB. Docker uses ~4GB. We give Spark 2GB max.
        # In production this would be 10-100x larger.
        .config("spark.driver.memory", "2g")

        # ---------- JARs ----------
        .config("spark.jars", jar_paths)

        # ---------- MinIO / S3A configuration ----------
        # These tell Hadoop's S3A connector to talk to MinIO instead of real AWS
        .config("spark.hadoop.fs.s3a.endpoint", minio_endpoint)
        .config("spark.hadoop.fs.s3a.access.key", minio_access_key)
        .config("spark.hadoop.fs.s3a.secret.key", minio_secret_key)

        # path.style.access = true: Use http://host/bucket/key format
        # (MinIO doesn't support the virtual-host style that real S3 uses)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")

        # Use the simple file system (no checksums, no version markers)
        # S3AFileSystem is the connector class in hadoop-aws JAR
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

        # Disable SSL since MinIO runs on plain HTTP locally
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")

        # ---------- Streaming safety ----------
        # Never infer schema in streaming — always define it explicitly
        .config("spark.sql.streaming.schemaInference", "false")

        # ---------- Parquet settings ----------
        # Write timestamps as INT96 for BigQuery compatibility later
        .config("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MICROS")

        .master("local[2]")  # 2 cores: 1 for reading Kafka, 1 for writing
        .getOrCreate()
    )

    # Reduce Spark's default logging noise (it's VERY verbose)
    spark.sparkContext.setLogLevel("WARN")

    return spark
