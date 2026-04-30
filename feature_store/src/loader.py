"""
Feature Loader: Postgres Gold Layer → Redis

Reads pre-computed fraud features from the gold layer in Postgres
and loads them into Redis as JSON for sub-millisecond serving.
"""

import json
import logging
import os
from datetime import datetime, date
from decimal import Decimal

import psycopg2
import redis

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger("feature_loader")


class DecimalEncoder(json.JSONEncoder):
    """Handle Decimal, datetime, and date types from Postgres."""
    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)
        if isinstance(obj, (datetime, date)):
            return obj.isoformat()
        return super().default(obj)


def get_postgres_connection():
    return psycopg2.connect(
        host=os.getenv("PG_HOST", "127.0.0.1"),
        port=int(os.getenv("PG_PORT", "5432")),
        dbname=os.getenv("PG_DATABASE", "fraud_reference"),
        user=os.getenv("PG_USER", "fraud_admin"),
        password=os.getenv("PG_PASSWORD", "changeme_local_only"),
    )


def get_redis_connection():
    return redis.Redis(
        host=os.getenv("REDIS_HOST", "127.0.0.1"),
        port=int(os.getenv("REDIS_PORT", "6379")),
        db=0,
        decode_responses=True,
    )


def load_user_features(pg_conn, redis_conn):
    logger.info("Loading user fraud features...")
    cur = pg_conn.cursor()
    cur.execute("SELECT * FROM silver_gold.gold_user_fraud_features")
    columns = [desc[0] for desc in cur.description]
    loaded = 0
    pipeline = redis_conn.pipeline()

    for row in cur:
        record = dict(zip(columns, row))
        user_id = record["user_id"]
        key = f"user:features:{user_id}"
        value = json.dumps(record, cls=DecimalEncoder)
        pipeline.set(key, value)
        pipeline.expire(key, 86400)
        loaded += 1
        if loaded % 100 == 0:
            pipeline.execute()
            logger.info(f"  Loaded {loaded} users...")

    pipeline.execute()
    cur.close()
    logger.info(f"Loaded {loaded} user feature vectors into Redis")
    return loaded


def load_merchant_stats(pg_conn, redis_conn):
    logger.info("Loading merchant daily stats...")
    cur = pg_conn.cursor()
    cur.execute("SELECT * FROM silver_gold.gold_daily_merchant_stats")
    columns = [desc[0] for desc in cur.description]
    loaded = 0
    pipeline = redis_conn.pipeline()
    merchant_latest = {}

    for row in cur:
        record = dict(zip(columns, row))
        merchant_id = record["merchant_id"]
        event_date = str(record["event_date"])
        key = f"merchant:stats:{merchant_id}:{event_date}"
        value = json.dumps(record, cls=DecimalEncoder)
        pipeline.set(key, value)
        pipeline.expire(key, 604800)

        if merchant_id not in merchant_latest or event_date > merchant_latest[merchant_id][0]:
            merchant_latest[merchant_id] = (event_date, value)

        loaded += 1
        if loaded % 100 == 0:
            pipeline.execute()

    for merchant_id, (dt, value) in merchant_latest.items():
        pipeline.set(f"merchant:latest:{merchant_id}", value)
        pipeline.expire(f"merchant:latest:{merchant_id}", 86400)

    pipeline.execute()
    cur.close()
    logger.info(f"Loaded {loaded} merchant stat records into Redis")
    return loaded


def set_metadata(redis_conn, user_count, merchant_count):
    meta = {
        "last_loaded_at": datetime.utcnow().isoformat(),
        "user_features_count": user_count,
        "merchant_stats_count": merchant_count,
        "loader_version": "1.0.0",
    }
    redis_conn.set("_meta:features:last_loaded", json.dumps(meta))
    logger.info(f"Metadata updated: {meta}")


def run():
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
