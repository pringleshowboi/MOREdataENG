#!/bin/bash

# --- 1. Environment Setup ---

# Create required directories if they don't exist
DIRS="dags logs plugins pg_data"
echo "Creating required directories: $DIRS..."
mkdir -p $DIRS

# Check if the AIRFLOW_UID environment variable is set.
# This is crucial for fixing 'exit 127' and permission issues on Linux/WSL/Mac.
if [ -z "$AIRFLOW_UID" ]; then
    # If running on Linux/WSL/Mac, use the current user's UID to prevent permission errors
    if command -v id > /dev/null 2>&1 && [ "$(uname -s)" != "Windows_NT" ]; then
        AIRFLOW_UID=$(id -u)
        echo "Setting AIRFLOW_UID environment variable to current user ID: $AIRFLOW_UID"
    else
        # Default UID for Windows or if id command is unavailable
        AIRFLOW_UID=50000
        echo "Setting default AIRFLOW_UID environment variable for Windows/Generic: $AIRFLOW_UID"
    fi
fi

# Export the variable so docker-compose can use it
export AIRFLOW_UID=$AIRFLOW_UID

# --- 2. Container Orchestration ---

echo "Starting Airflow, Kafka, Redis, and PostgreSQL containers..."

# Use 'docker compose' (modern syntax) to build/start services in detached mode
# Note: We use 'up' to run the 'airflow-init' service first.
docker compose -f docker-compose.yml up --wait --detach

# Check if the initialization service succeeded
if [ $? -eq 0 ]; then
    echo "---"
    echo "Infrastructure setup completed successfully."
    echo "Access Airflow UI at: http://localhost:8081 (Username: admin, Password: admin)"
    echo "Kafka broker accessible at: localhost:9092"
    echo "---"
    echo "Next Step: Install Feast and define your features."
else
    echo "---"
    echo "ERROR: Docker Compose failed to start all services."
    echo "Please check the logs for the 'airflow-init' service."
    echo "You may need to run 'docker compose logs airflow-init' for full error details."
    echo "---"
fi

# Clean up the exported variable
unset AIRFLOW_UID