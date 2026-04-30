"""
fraud_pipeline_dag.py — Main Orchestration DAG

This DAG orchestrates the entire fraud data pipeline:
    1. dbt snapshot  → SCD Type 2 updates for merchants/users
    2. dbt run       → Build all silver + gold models
    3. dbt test      → Data quality gates (blocks downstream if fails)
    4. Feature load  → Push gold features to Redis for real-time serving

Schedule: Every 4 hours (6 times per day)
    Why 4 hours? Fraud features need to be reasonably fresh, but
    running every minute would overload the warehouse. 4 hours is
    the standard batch refresh interval at most fintechs. Real-time
    features are handled separately via Spark Streaming.

Interview talking point:
    "I orchestrated the pipeline with Airflow using task dependencies
    that enforce data quality gates. If dbt tests fail, the feature
    loader is skipped — stale-but-correct features in Redis are better
    than fresh-but-corrupt features. The DAG has retry logic, SLA
    monitoring, and email alerting on failure."
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


# ---------- Default Arguments ----------
# These apply to every task in the DAG unless overridden.
default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,

    # Retry logic: if a task fails, retry 2 times with 5-minute gaps
    # Why? Transient failures (network blip, Postgres temp lock) are common.
    # Permanent failures still fail after retries, and alerting kicks in.
    "retries": 2,
    "retry_delay": timedelta(minutes=5),

    # SLA: if a task hasn't completed within 30 minutes, send an alert.
    # This catches "stuck" tasks (e.g., dbt run hanging on a lock).
    "sla": timedelta(minutes=30),

    # Email alerting (configure SMTP in airflow.cfg for production)
    "email_on_failure": True,
    "email_on_retry": False,
}


# ---------- DAG Definition ----------
with DAG(
    dag_id="fraud_feature_pipeline",
    default_args=default_args,
    description="Orchestrates dbt transformations and feature store loading",
    # Run every 4 hours starting at midnight
    schedule_interval="0 */4 * * *",
    start_date=datetime(2026, 4, 1),
    catchup=False,  # Don't backfill historical runs
    tags=["fraud", "dbt", "feature-store"],
    doc_md="""
    ## Fraud Feature Pipeline

    **Owner:** Data Engineering

    **Schedule:** Every 4 hours

    **Flow:**
    ```
    dbt_snapshot → dbt_run → dbt_test → load_features_to_redis
    ```

    **Data Quality Gate:** If `dbt_test` fails, `load_features_to_redis`
    is skipped. This prevents corrupt features from reaching the
    fraud scoring API.
    """,
) as dag:

    # ---------- dbt Project Paths ----------
    # Inside the Airflow container, dbt project is mounted at /opt/airflow/dbt
    DBT_PROJECT_DIR = "/opt/airflow/dbt"
    DBT_PROFILES_DIR = "/opt/airflow/dags"

    # Common dbt command prefix
    DBT_CMD = f"cd {DBT_PROJECT_DIR} && dbt"
    DBT_FLAGS = f"--profiles-dir {DBT_PROFILES_DIR}"

    # ============================================================
    # Task 1: dbt snapshot (SCD Type 2)
    # ============================================================
    # Updates the merchant and user dimension snapshots.
    # If a merchant's risk tier changed since last run, a new
    # versioned row is created with validity timestamps.
    dbt_snapshot = BashOperator(
        task_id="dbt_snapshot",
        bash_command=f"{DBT_CMD} snapshot {DBT_FLAGS}",
        doc_md="Run dbt snapshots for SCD Type 2 on merchants and users.",
    )

    # ============================================================
    # Task 2: dbt run (build all models)
    # ============================================================
    # Builds silver staging views and gold tables in dependency order.
    # dbt resolves the DAG automatically:
    #   stg_transactions → stg_merchants → stg_users →
    #   int_transactions_enriched → gold_user_fraud_features →
    #   gold_daily_merchant_stats
    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"{DBT_CMD} run {DBT_FLAGS}",
        doc_md="Build all dbt models (silver + gold layers).",
    )

    # ============================================================
    # Task 3: dbt test (data quality gate)
    # ============================================================
    # Runs all 46 data quality tests. If ANY test fails, this task
    # fails and downstream tasks are skipped.
    #
    # This is the GATE — it prevents corrupt data from reaching
    # the feature store and fraud scoring API.
    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"{DBT_CMD} test {DBT_FLAGS}",
        doc_md="Run all 46 data quality tests. Blocks downstream if any fail.",
    )

    # ============================================================
    # Task 4: Load features to Redis
    # ============================================================
    # Reads gold layer features from Postgres and loads them into
    # Redis for sub-millisecond serving by the FastAPI feature API.
    #
    # This task ONLY runs if dbt_test passes. If tests fail,
    # we keep the old (correct) features in Redis rather than
    # loading potentially corrupt new ones.
    def load_features():
        """Load gold features from Postgres into Redis."""
        import json
        import os
        from datetime import datetime, date
        from decimal import Decimal

        import psycopg2
        import redis

        class Encoder(json.JSONEncoder):
            def default(self, obj):
                if isinstance(obj, Decimal):
                    return float(obj)
                if isinstance(obj, (datetime, date)):
                    return obj.isoformat()
                return super().default(obj)

        pg = psycopg2.connect(
            host=os.getenv("PG_HOST", "postgres"),
            port=int(os.getenv("PG_PORT", "5432")),
            dbname=os.getenv("PG_DATABASE", "fraud_reference"),
            user=os.getenv("PG_USER", "fraud_admin"),
            password=os.getenv("PG_PASSWORD", "changeme_local_only"),
        )
        r = redis.Redis(
            host=os.getenv("REDIS_HOST", "redis"),
            port=int(os.getenv("REDIS_PORT", "6379")),
            db=0,
            decode_responses=True,
        )

        # Load user features
        cur = pg.cursor()
        cur.execute("SELECT * FROM silver_gold.gold_user_fraud_features")
        cols = [d[0] for d in cur.description]
        pipe = r.pipeline()
        count = 0
        for row in cur:
            rec = dict(zip(cols, row))
            key = f"user:features:{rec['user_id']}"
            pipe.set(key, json.dumps(rec, cls=Encoder))
            pipe.expire(key, 86400)
            count += 1
            if count % 100 == 0:
                pipe.execute()
        pipe.execute()
        cur.close()
        print(f"Loaded {count} user feature vectors")

        # Load merchant stats
        cur = pg.cursor()
        cur.execute("SELECT * FROM silver_gold.gold_daily_merchant_stats")
        cols = [d[0] for d in cur.description]
        merchant_latest = {}
        mcount = 0
        for row in cur:
            rec = dict(zip(cols, row))
            mid = rec["merchant_id"]
            edate = str(rec["event_date"])
            key = f"merchant:stats:{mid}:{edate}"
            val = json.dumps(rec, cls=Encoder)
            pipe.set(key, val)
            pipe.expire(key, 604800)
            if mid not in merchant_latest or edate > merchant_latest[mid][0]:
                merchant_latest[mid] = (edate, val)
            mcount += 1
            if mcount % 100 == 0:
                pipe.execute()

        for mid, (dt, val) in merchant_latest.items():
            pipe.set(f"merchant:latest:{mid}", val)
            pipe.expire(f"merchant:latest:{mid}", 86400)
        pipe.execute()
        cur.close()
        print(f"Loaded {mcount} merchant stat records")

        # Metadata
        meta = {
            "last_loaded_at": datetime.utcnow().isoformat(),
            "user_features_count": count,
            "merchant_stats_count": mcount,
            "loader_version": "1.0.0",
            "loaded_by": "airflow",
        }
        r.set("_meta:features:last_loaded", json.dumps(meta))
        print(f"Feature load complete: {meta}")

        pg.close()
        r.close()

    load_features_to_redis = PythonOperator(
        task_id="load_features_to_redis",
        python_callable=load_features,
        doc_md="Load gold features from Postgres into Redis for real-time serving.",
    )

    # ============================================================
    # Task Dependencies (the DAG structure)
    # ============================================================
    # snapshot → run → test → load
    #
    # If test fails, load is skipped.
    # If run fails, test and load are skipped.
    # If snapshot fails, everything downstream is skipped.
    dbt_snapshot >> dbt_run >> dbt_test >> load_features_to_redis
