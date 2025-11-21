#!/bin/bash
# ml_flow_entrypoint.sh

# --- Start Database Wait Loop ---
# Assuming the Postgres service name in docker-compose.yml is 'postgres'
DB_HOST="postgres"
DB_PORT="5432"

echo "Waiting for PostgreSQL at $DB_HOST:$DB_PORT..."

# Wait until Netcat (nc) can successfully connect to the DB host/port.
until nc -z $DB_HOST $DB_PORT; do
  printf '.'
  sleep 1
done

echo "PostgreSQL is available. Starting MLflow server."
# --- End Database Wait Loop ---

# Execute the main MLflow server command
exec mlflow server \
    --host 0.0.0.0 \
    --port 5000 \
    --backend-store-uri $MLFLOW_DATABASE_URI \
    --default-artifact-root $MLFLOW_ARTIFACT_ROOT