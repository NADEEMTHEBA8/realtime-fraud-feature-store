"""
Feature Loader: Postgres Gold Layer → Redis

What this does:
    Reads pre-computed fraud features from the gold layer in Postgres
    and loads them into Redis as JSON hashes for sub-millisecond serving.

Why Redis?
    A fraud scoring API needs to respond in <10ms. Postgres can do 5-20ms
    for a simple key lookup, but Redis does it in <1ms. When you're scoring
    thousands of transactions per second, those milliseconds matter.

Redis key design:
    user:features:{user_id} → JSON hash of all 25+ features
    merchant:stats:{merchant_id}:{date} → JSON hash of daily merchant stats
    _meta:features:last_loaded → timestamp of last load (for freshness checks)

Interview talking point:
    "I built a feature loader that materializes the dbt gold layer into Redis
    for sub-millisecond serving. The same features used for batch model training
    in Postgres are served in real-time from Redis, ensuring consistency between
    training and scoring — avoiding the training-serving skew problem."
"""

import json
import logging
import os
from datetime import datetime
from decimal import Decimal

import psycopg2
import redis

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger("feature_loader")


class DecimalEncoder(json.JSONEncoder):
    """Handle Decimal types from Postgres NUMERIC columns."""
    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)
        if isinstance(obj, datetime):
            return obj.isoformat()
        return super().default(obj)


def get_postgres_connection():
    """Connect to Postgres using the same credentials as dbt."""
    return psycopg2.connect(
        host=os.getenv("PG_HOST", "127.0.0.1"),
        port=int(os.getenv("PG_PORT", "5432")),
        dbname=os.getenv("PG_DATABASE", "fraud_reference"),
        user=os.getenv("PG_USER", "fraud_admin"),
        password=os.getenv("PG_PASSWORD", "changeme_local_only"),
    )


def get_redis_connection():
    """Connect to Redis."""
    return redis.Redis(
        host=os.getenv("REDIS_HOST", "127.0.0.1"),
        port=int(os.getenv("REDIS_PORT", "6379")),
        db=0,
        decode_responses=True,
    )


def load_user_features(pg_conn, redis_conn):
    """
    Load user fraud features from Postgres gold layer into Redis.

    Each user gets a Redis key: user:features:{user_id}
    Value is a JSON string of all 25+ features.

    Why JSON string (not Redis HSET)?
        A single GET is faster than HGETALL for serving all features
        at once. The fraud scoring API needs ALL features for a user
        in one call, so atomic GET of a JSON blob is optimal.
    """
    logger.info("Loading user fraud features...")

    cur = pg_conn.cursor()
    cur.execute("""
        SELECT * FROM silver_gold.gold_user_fraud_features
    """)

    columns = [desc[0] for desc in cur.description]
    loaded = 0
    pipeline = redis_conn.pipeline()

    for row in cur:
        record = dict(zip(columns, row))
        user_id = record["user_id"]
        key = f"user:features:{user_id}"

        # Serialize to JSON (handles Decimal and datetime)
        value = json.dumps(record, cls=DecimalEncoder)

        # Use pipeline for batch writes (much faster than individual SETs)
        pipeline.set(key, value)

        # Set TTL of 24 hours — features should be refreshed daily
        # If the loader doesn't run, stale features expire rather than
        # serving outdated data silently
        pipeline.expire(key, 86400)

        loaded += 1

        # Execute pipeline in batches of 100
        if loaded % 100 == 0:
            pipeline.execute()
            logger.info(f"  Loaded {loaded} users...")

    # Execute remaining commands in pipeline
    pipeline.execute()
    cur.close()

    logger.info(f"Loaded {loaded} user feature vectors into Redis")
    return loaded


def load_merchant_stats(pg_conn, redis_conn):
    """
    Load daily merchant stats from Postgres gold layer into Redis.

    Each merchant-date combo gets a key: merchant:stats:{merchant_id}:{date}
    Also stores latest stats: merchant:latest:{merchant_id}
    """
    logger.info("Loading merchant daily stats...")

    cur = pg_conn.cursor()
    cur.execute("""
        SELECT * FROM silver_gold.gold_daily_merchant_stats
    """)

    columns = [desc[0] for desc in cur.description]
    loaded = 0
    pipeline = redis_conn.pipeline()

    # Track latest date per merchant for the "latest" key
    merchant_latest = {}

    for row in cur:
        record = dict(zip(columns, row))
        merchant_id = record["merchant_id"]
        event_date = str(record["event_date"])
        key = f"merchant:stats:{merchant_id}:{event_date}"

        value = json.dumps(record, cls=DecimalEncoder)
        pipeline.set(key, value)
        pipeline.expire(key, 604800)  # 7-day TTL for historical stats

        # Track the latest date for each merchant
        if merchant_id not in merchant_latest or event_date > merchant_latest[merchant_id][0]:
            merchant_latest[merchant_id] = (event_date, value)

        loaded += 1

        if loaded % 100 == 0:
            pipeline.execute()

    # Store "latest" key for each merchant (most recent day's stats)
    for merchant_id, (date, value) in merchant_latest.items():
        pipeline.set(f"merchant:latest:{merchant_id}", value)
        pipeline.expire(f"merchant:latest:{merchant_id}", 86400)

    pipeline.execute()
    cur.close()

    logger.info(f"Loaded {loaded} merchant stat records into Redis")
    return loaded


def set_metadata(redis_conn, user_count, merchant_count):
    """
    Store metadata about the last load for freshness monitoring.

    The API health endpoint checks this to verify features are fresh.
    If _meta:features:last_loaded is older than 25 hours, the health
    check returns degraded status.
    """
    meta = {
        "last_loaded_at": datetime.utcnow().isoformat(),
        "user_features_count": user_count,
        "merchant_stats_count": merchant_count,
        "loader_version": "1.0.0",
    }
    redis_conn.set("_meta:features:last_loaded", json.dumps(meta))
    logger.info(f"Metadata updated: {meta}")


def run():
    """Main entry point for the feature loader."""
    logger.info("=" * 60)
    logger.info("FEATURE LOADER STARTING")
    logger.info("=" * 60)

    pg_conn = get_postgres_connection()
    redis_conn = get_redis_connection()

    try:
        user_count = load_user_features(pg_conn, redis_conn)
        merchant_count = load_merchant_stats(pg_conn, redis_conn)
        set_metadata(redis_conn, user_count, merchant_count)

        logger.info("=" * 60)
        logger.info("FEATURE LOADER COMPLETE")
        logger.info(f"  Users:     {user_count}")
        logger.info(f"  Merchants: {merchant_count}")
        logger.info("=" * 60)

    finally:
        pg_conn.close()
        redis_conn.close()


if __name__ == "__main__":
    run()
