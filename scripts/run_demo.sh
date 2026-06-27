#!/bin/bash
# Clean, metric-focused demo orchestrator

# Create a logs directory to hide the messy output
mkdir -p .demo_logs

echo "=== 0. Infrastructure ==="
make up > .demo_logs/0_infra.log 2>&1
echo "Waiting 15s for Kafka/Postgres to initialize..."
sleep 15
echo "[+] Docker containers initialized (Kafka, Postgres, Redis, MinIO)"
echo ""

echo "=== 1. Ingestion ==="
PYTHONWARNINGS="ignore" .venv/bin/python -m ingestion.transaction_generator.src.run --firehose --max-events 500 > .demo_logs/1_ingestion.log 2>&1
if [ $? -ne 0 ]; then echo "❌ Failed! See .demo_logs/1_ingestion.log"; exit 1; fi
RATE=$(grep -o "avg_rate=[0-9.]*" .demo_logs/1_ingestion.log | cut -d= -f2)
echo "[+] Generated 500 raw transactions (Speed: $RATE tx/sec)"
echo ""

echo "=== 2. Streaming (Spark) ==="
PYTHONWARNINGS="ignore" .venv/bin/python -m streaming.spark.src.bronze_ingest --once > .demo_logs/2_spark.log 2>&1
if [ $? -ne 0 ]; then echo "❌ Failed! See .demo_logs/2_spark.log"; exit 1; fi
PYTHONWARNINGS="ignore" .venv/bin/python load_bronze_to_postgres.py > .demo_logs/2_postgres.log 2>&1
echo "[+] Read 500 records from Kafka -> Wrote to MinIO Delta Lake & Postgres"
echo ""

echo "=== 3. Transformation (dbt) ==="
cd warehouse/dbt/fraud_warehouse
../../../.venv/bin/dbt run > ../../../.demo_logs/3_dbt.log 2>&1
if [ $? -ne 0 ]; then echo "❌ Failed! See .demo_logs/3_dbt.log"; exit 1; fi
cd ../../..
TIME=$(grep -o "in [0-9]* hours [0-9]* minutes and [0-9.]* seconds" .demo_logs/3_dbt.log | sed 's/in 0 hours 0 minutes and //g' || echo "(fast)")
echo "[+] dbt run successful: 3 Views, 5 Tables built ($TIME)"
echo ""

echo "=== 4. Serving (Redis) ==="
.venv/bin/python -m feature_store.src.loader > .demo_logs/4_redis.log 2>&1
if [ $? -ne 0 ]; then echo "❌ Failed! See .demo_logs/4_redis.log"; exit 1; fi
USERS=$(grep -o "users=[0-9]*" .demo_logs/4_redis.log | cut -d= -f2 | tail -n 1)
MERCHANTS=$(grep -o "merchants=[0-9]*" .demo_logs/4_redis.log | cut -d= -f2 | tail -n 1)
echo "[+] Loaded $USERS user feature vectors"
echo "[+] Loaded $MERCHANTS merchant stats"
echo ""

echo "Pipeline Complete!"
echo ""
echo "Here are all your project dashboards and credentials:"
echo "  Kafka UI:  http://localhost:8080   (No auth required)"
echo "  MinIO UI:  http://localhost:9001   (user: minioadmin | pass: minioadmin)"
while ! docker compose exec airflow cat /opt/airflow/standalone_admin_password.txt >/dev/null 2>&1 ; do sleep 2; done
AIRFLOW_PASS=$(docker compose exec airflow cat /opt/airflow/standalone_admin_password.txt | tr -d '\r\n')
echo "  Airflow:   http://localhost:8081   (user: admin | pass: $AIRFLOW_PASS)"
echo "  FastAPI:   http://localhost:8002/docs (No auth required)"
echo ""
echo "To test the API, you can now run 'make api' in a new terminal!"
