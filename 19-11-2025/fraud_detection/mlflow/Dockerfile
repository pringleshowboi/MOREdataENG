FROM python:3.10-slim

# Removed boto3 (AWS) to save space for Zero-Cost/Low-Resource goal
RUN pip install --no-cache-dir \
    mlflow>=2.8.0 \
    psycopg2-binary>=2.9.0 \
    azure-storage-blob>=12.0.0 \
    azure-identity>=1.12.0

EXPOSE 5000

CMD ["mlflow", "server", "--host", "0.0.0.0", "--port", "5000"]