#!/usr/bin/env bash
# Registers the Debezium Postgres source connector with Kafka Connect.
# Idempotent: PUT .../config creates or updates the connector.
#
# Run after `docker compose up -d` and after the reference tables are seeded
# (make seed). Requires python3 on the host.

set -euo pipefail

CONNECT_URL="${CONNECT_URL:-http://localhost:8083}"
CONFIG_FILE="$(dirname "$0")/postgres-source.json"
CONNECTOR_NAME="reference-postgres-source"

echo "Waiting for Kafka Connect at ${CONNECT_URL}..."
until curl -sf "${CONNECT_URL}/connectors" >/dev/null 2>&1; do
  sleep 2
done

# PUT /connectors/{name}/config expects the bare config object, not the wrapper.
python3 -c "import json,sys; print(json.dumps(json.load(open(sys.argv[1]))['config']))" "$CONFIG_FILE" \
  | curl -sf -X PUT -H "Content-Type: application/json" --data @- \
    "${CONNECT_URL}/connectors/${CONNECTOR_NAME}/config" -o /dev/null

echo "Connector '${CONNECTOR_NAME}' registered. Status:"
curl -s "${CONNECT_URL}/connectors/${CONNECTOR_NAME}/status" | python3 -m json.tool
