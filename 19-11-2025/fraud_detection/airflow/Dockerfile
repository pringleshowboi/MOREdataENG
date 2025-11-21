# Base image matches your configuration
ARG AIRFLOW_VERSION=3.1.3
FROM apache/airflow:${AIRFLOW_VERSION}

# Switch to root to install system dependencies (like build tools)
USER root
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    build-essential \
    # Clean up APT cache to keep the image size down
    && rm -rf /var/lib/apt/lists/*

# Switch back to the 'airflow' user to ensure correct permissions
USER airflow

# Copy the requirements file into the working directory
COPY requirements.txt .

# Install Python dependencies
# The --no-cache-dir flag helps minimize the final image size
RUN pip install --no-cache-dir -r requirements.txt