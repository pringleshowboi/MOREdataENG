from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from datetime import datetime, timedelta # Corrected for days_ago error
from docker.types import Mount
import os

# Get the host project directory from the environment variable we set in docker-compose
PROJECT_DIR = os.getenv("PROJECT_DIR")

default_args = {
    'owner': 'security_team',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

with DAG(
    'security_log_processing',
    default_args=default_args,
    description='A pipeline to process security logs using Spark',
    schedule='*/10 * * * *', # Corrected schedule_interval to schedule
    start_date=datetime.now() - timedelta(days=1), # Corrected start_date calculation
    catchup=False,
    tags=['security', 'spark'],
) as dag:

    # Task: Run Spark Job in a Docker Container
    process_logs = DockerOperator(
        task_id='spark_transform_job',
        image='apache/spark:3.5.0',
        container_name='airflow_spark_job',
        api_version='auto',
        auto_remove='force', # Corrected boolean True to string 'force'
        
        # We must run as root to avoid permission issues with the mounted volume
        user='root', 
        
        # Mount the scripts directory from your Host machine into the container
        mounts=[
            Mount(
                source=f"{PROJECT_DIR}/scripts", 
                target="/app/scripts", 
                type="bind"
            )
        ],
        
        # The Spark Submit Command
        command=(
            "/opt/spark/bin/spark-submit "
            "--master local[*] "
            "/app/scripts/spark_processor.py"
        ),
        
        # Network configuration so it can reach MinIO
        network_mode='15-12-2025_default', # IMPORTANT: Confirm this network name matches your docker-compose environment!
        environment={
            'SPARK_MODE': 'standalone',
            'MINIO_ENDPOINT': 'minio:9000',
            'MINIO_ACCESS_KEY': 'minio',
            'MINIO_SECRET_KEY': 'minio123'
        }
    )

    process_logs