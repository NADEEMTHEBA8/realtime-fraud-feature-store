"""
Feature loader: Postgres gold layer -> Redis.

Materializes gold_user_fraud_features and gold_daily_merchant_stats into
Redis keys for the serving API. Run standalone (`make features`) or as the
final task of the Airflow DAG.

Keys:
    user:features:{user_id}                JSON, all user features, 24h TTL
    merchant:stats:{merchant_id}:{date}    JSON, daily merchant stats, 7d TTL
    merchant:latest:{merchant_id}          JSON, most recent day, 24h TTL
    _meta:features:last_loaded             load metadata for /health
"""

import json
import logging
import os
from datetime import date, datetime
from decimal import Decimal

import psycopg2
import redis

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("feature_loader")

USER_TTL_SECONDS = 86400      # 24h
MERCHANT_TTL_SECONDS = 604800  # 7d
PIPELINE_BATCH = 100


class _JsonEncoder(json.JSONEncoder):
    """Postgres NUMERIC -> float, date/datetime -> ISO string."""

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


def load_user_features(pg_conn, redis_conn) -> int:
    """Load gold_user_fraud_features into user:features:{user_id} keys."""
    cur = pg_conn.cursor()
    cur.execute("SELECT * FROM silver_gold.gold_user_fraud_features")
    columns = [d[0] for d in cur.description]

    pipe = redis_conn.pipeline()
    loaded = 0
    for row in cur:
        record = dict(zip(columns, row))
        key = f"user:features:{record['user_id']}"
        pipe.set(key, json.dumps(record, cls=_JsonEncoder))
        pipe.expire(key, USER_TTL_SECONDS)
        loaded += 1
        if loaded % PIPELINE_BATCH == 0:
            pipe.execute()
    pipe.execute()
    cur.close()

    logger.info("Loaded %d user feature vectors", loaded)
    return loaded


def load_merchant_stats(pg_conn, redis_conn) -> int:
    """Load gold_daily_merchant_stats; also write a per-merchant latest key."""
    cur = pg_conn.cursor()
    cur.execute("SELECT * FROM silver_gold.gold_daily_merchant_stats")
    columns = [d[0] for d in cur.description]

    pipe = redis_conn.pipeline()
    latest: dict[str, tuple[str, str]] = {}
    loaded = 0
    for row in cur:
        record = dict(zip(columns, row))
        merchant_id = record["merchant_id"]
        event_date = str(record["event_date"])
        value = json.dumps(record, cls=_JsonEncoder)

        key = f"merchant:stats:{merchant_id}:{event_date}"
        pipe.set(key, value)
        pipe.expire(key, MERCHANT_TTL_SECONDS)

        if merchant_id not in latest or event_date > latest[merchant_id][0]:
            latest[merchant_id] = (event_date, value)

        loaded += 1
        if loaded % PIPELINE_BATCH == 0:
            pipe.execute()

    for merchant_id, (_, value) in latest.items():
        key = f"merchant:latest:{merchant_id}"
        pipe.set(key, value)
        pipe.expire(key, USER_TTL_SECONDS)
    pipe.execute()
    cur.close()

    logger.info("Loaded %d merchant stat records", loaded)
    return loaded


def set_metadata(redis_conn, user_count: int, merchant_count: int) -> None:
    """Write load metadata; the API /health endpoint reads this for freshness."""
    meta = {
        "last_loaded_at": datetime.utcnow().isoformat(),
        "user_features_count": user_count,
        "merchant_stats_count": merchant_count,
        "loader_version": "1.0.0",
    }
    redis_conn.set("_meta:features:last_loaded", json.dumps(meta))


def run() -> None:
    pg_conn = get_postgres_connection()
    redis_conn = get_redis_connection()
    try:
        user_count = load_user_features(pg_conn, redis_conn)
        merchant_count = load_merchant_stats(pg_conn, redis_conn)
        set_metadata(redis_conn, user_count, merchant_count)
        logger.info(
            "Feature load complete: users=%d merchants=%d",
            user_count,
            merchant_count,
        )
    finally:
        pg_conn.close()
        redis_conn.close()


if __name__ == "__main__":
    run()
