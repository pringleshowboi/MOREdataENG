FROM apache/airflow:3.1.3

# This section runs as 'root' and is used to install system-level dependencies.
USER root
RUN apt-get update && apt-get install -y \
    build-essential \
    default-libmysqlclient-dev \
    libssl-dev \
    libffi-dev \
    libpq-dev \
    git \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Copy the Python requirements file into a temporary location
COPY requirements.txt /tmp/requirements.txt

# CRITICAL FIX: Switch to the non-root 'airflow' user before installing Python packages
# This resolves the "You are running pip as root" error.
USER airflow

# Install Python dependencies using the non-root user
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# The final image defaults back to the 'airflow' user if no other USER command is specified.