FROM python:3.10-slim

# Install system dependencies including procps for ps command and curl for healthcheck
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    libpq-dev \
    gcc \
    curl \
    procps \
    && rm -rf /var/lib/apt/lists/*

# Install Python packages
RUN pip install --no-cache-dir \
    mlflow>=2.8.0 \
    psycopg2-binary>=2.9.0 \
    azure-storage-blob>=12.0.0 \
    azure-identity>=1.12.0

WORKDIR /mlflow

# Expose MLflow port
EXPOSE 5000

# The CMD will be specified in docker-compose.yml