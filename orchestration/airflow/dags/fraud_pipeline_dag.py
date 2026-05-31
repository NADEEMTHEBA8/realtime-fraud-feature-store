"""
Batch DAG: dbt snapshot -> dbt run -> dbt test -> load features to Redis.

dbt_test is a gate. If any test fails, the feature load is skipped and the
previous (passing) feature set stays in Redis until its TTL expires —
stale-but-validated is preferred over fresh-but-broken.

The streaming side (Kafka -> Spark -> bronze) runs independently and is not
orchestrated here.
"""

import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "sla": timedelta(minutes=30),
    # No SMTP is configured for this local setup; SLA misses are visible in
    # the Airflow UI rather than emailed.
    "email_on_failure": False,
    "email_on_retry": False,
}

with DAG(
    dag_id="fraud_feature_pipeline",
    default_args=default_args,
    description="dbt transformations and feature store load",
    schedule_interval="0 */4 * * *",
    start_date=datetime(2026, 4, 1),
    catchup=False,
    tags=["fraud", "dbt", "feature-store"],
) as dag:

    # dbt project + profile are mounted into the Airflow container.
    DBT_PROJECT_DIR = "/opt/airflow/dbt"
    DBT_PROFILES_DIR = "/opt/airflow/dags"
    DBT_CMD = f"cd {DBT_PROJECT_DIR} && dbt"
    DBT_FLAGS = f"--profiles-dir {DBT_PROFILES_DIR}"

    dbt_snapshot = BashOperator(
        task_id="dbt_snapshot",
        bash_command=f"{DBT_CMD} snapshot {DBT_FLAGS}",
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"{DBT_CMD} run {DBT_FLAGS}",
    )

    # Quality gate: a failed test fails this task and skips the feature load.
    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"{DBT_CMD} test {DBT_FLAGS}",
    )

    def load_features() -> None:
        """Run the feature loader (Postgres gold -> Redis).

        Reuses feature_store.src.loader so the batch path and the standalone
        `make features` path stay identical. /opt/airflow holds the mounted
        feature_store package; PG_*/REDIS_* env vars are set in compose.
        """
        if "/opt/airflow" not in sys.path:
            sys.path.insert(0, "/opt/airflow")
        from feature_store.src.loader import run as run_loader

        run_loader()

    load_features_to_redis = PythonOperator(
        task_id="load_features_to_redis",
        python_callable=load_features,
    )

    dbt_snapshot >> dbt_run >> dbt_test >> load_features_to_redis
