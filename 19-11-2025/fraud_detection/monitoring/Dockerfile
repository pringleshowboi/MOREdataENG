# Use a lightweight official Python image
FROM python:3.11-slim

# Set the working directory inside the container
WORKDIR /app

# Install system dependencies for PostgreSQL
RUN apt-get update && apt-get install -y \
    gcc \
    postgresql-client \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements file first to leverage Docker layer caching
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy the application code into the container
COPY monitoring_consumer.py .

# Create a non-root user for security
RUN useradd -m -u 1000 monitoring && chown -R monitoring:monitoring /app
USER monitoring

# Health check to ensure the service is running
HEALTHCHECK --interval=30s --timeout=10s --start-period=45s --retries=3 \
    CMD ps aux | grep -v grep | grep python || exit 1

# Command to run the monitoring consumer script
CMD ["python", "-u", "monitoring_consumer.py"]