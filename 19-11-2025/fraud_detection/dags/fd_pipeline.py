"""
Fraud Detection ML Pipeline (Real Data Version)
This DAG orchestrates the entire fraud detection workflow using historical data:
1. Data ingestion (Loads creditcard.csv from local volume)
2. Data preprocessing & Feature Engineering
3. Model training with MLflow tracking
4. Model evaluation
5. Model registration/deployment (ready for Kafka consumers)
"""

from datetime import datetime, timedelta
from airflow import DAG
# Updated imports for Airflow 2.8+ compatibility to avoid warnings
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.bash import BashOperator
# REMOVED: from airflow.utils.dates import days_ago 
# Fix: Removed the deprecated import to resolve the ImportError

import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import classification_report, roc_auc_score
from imblearn.over_sampling import SMOTE
import mlflow
import mlflow.sklearn
from mlflow.tracking.client import MlflowClient
import joblib
import os
import time

# --- Configuration ---
# MLflow configuration
MLFLOW_TRACKING_URI = os.getenv('MLFLOW_TRACKING_URI', 'http://mlflow-server:5000')
mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
MLFLOW_EXPERIMENT_NAME = "fraud_detection"
MLFLOW_MODEL_NAME = "FraudDetectionRFModel"

# Paths
DATA_DIR = '/opt/airflow/data'
MODEL_DIR = '/app/models'
RAW_CSV_SOURCE = '/opt/airflow/data/creditcard.csv'
# ---------------------

def download_sample_data(**context):
    """
    Loads the existing creditcard.csv file instead of generating fake data.
    Transforms the Kaggle dataset columns to match the pipeline's expected feature names.
    """
    print(f"Loading historical fraud data from: {RAW_CSV_SOURCE}")
    
    if not os.path.exists(RAW_CSV_SOURCE):
        raise FileNotFoundError(f"CRITICAL: The file {RAW_CSV_SOURCE} was not found. Is it in your local 'data' folder?")

    # Read the actual CSV
    df = pd.read_csv(RAW_CSV_SOURCE)
    
    # Optional: Sample data if dataset is too large for quick testing
    # df = df.sample(frac=0.1, random_state=42) 

    # --- Schema Mapping ---
    # Map Kaggle 'Class' to 'is_fraud'
    if 'Class' in df.columns:
        df = df.rename(columns={'Class': 'is_fraud'})
        
    # Map Kaggle 'Amount' to 'transaction_amount'
    if 'Amount' in df.columns:
        df = df.rename(columns={'Amount': 'transaction_amount'})
    
    # --- Feature Engineering for Compatibility ---
    # The pipeline expects 'hour_of_day' and 'day_of_week', but Kaggle has 'Time' (seconds).
    # We derive these to keep the downstream logic consistent.
    if 'Time' in df.columns:
        df['hour_of_day'] = (df['Time'] // 3600) % 24
        df['day_of_week'] = (df['Time'] // (3600 * 24)) % 7
    
    # Create dummy columns for features the pipeline expects but Kaggle data lacks.
    # In a real scenario, you'd map these to actual columns like V1-V28.
    # For now, we generate them to ensure the model training code (which expects these) works.
    np.random.seed(42)
    df['merchant_category'] = np.random.randint(1, 20, len(df))
    df['customer_age'] = np.random.randint(18, 90, len(df))

    # Save to the raw working directory for the next task
    os.makedirs(f'{DATA_DIR}/raw', exist_ok=True)
    output_path = f'{DATA_DIR}/raw/transactions.csv'
    df.to_csv(output_path, index=False)
    
    print(f"✅ Loaded and transformed {len(df)} transactions from {RAW_CSV_SOURCE}")
    print(f"✅ Fraud cases found: {df['is_fraud'].sum()}")
    
    # Push metadata to XCom
    context['task_instance'].xcom_push(key='data_path', value=output_path)

def preprocess_data(**context):
    """
    Load and preprocess the raw transaction data.
    """
    print("🔄 Preprocessing data...")
    
    data_path = context['task_instance'].xcom_pull(task_ids='download_data', key='data_path')
    df = pd.read_csv(data_path)
    
    df = df.dropna()
    
    # Feature engineering
    # These features are critical for the Random Forest model
    df['amount_log'] = np.log1p(df['transaction_amount'])
    df['is_night'] = df['hour_of_day'].apply(lambda x: 1 if x < 6 or x > 22 else 0)
    df['is_weekend'] = df['day_of_week'].apply(lambda x: 1 if x >= 5 else 0)
    
    os.makedirs(f'{DATA_DIR}/processed', exist_ok=True)
    output_path = f'{DATA_DIR}/processed/transactions_processed.csv'
    df.to_csv(output_path, index=False)
    
    print(f"✅ Preprocessed data saved to: {output_path}")
    context['task_instance'].xcom_push(key='processed_data_path', value=output_path)

def train_model(**context):
    """
    Train fraud detection model with MLflow tracking.
    """
    print("🤖 Training fraud detection model...")
    
    data_path = context['task_instance'].xcom_pull(task_ids='preprocess_data', key='processed_data_path')
    df = pd.read_csv(data_path)
    
    # Prepare features and target
    # Note: We are training on the derived features + simulated metadata.
    # Ideally, you'd include V1-V28 here if you wanted to use the full Kaggle power.
    feature_cols = ['transaction_amount', 'hour_of_day', 'day_of_week', 
                    'merchant_category', 'customer_age', 'amount_log', 
                    'is_night', 'is_weekend']
    X = df[feature_cols]
    y = df['is_fraud']
    
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42, stratify=y
    )
    
    # Scale features and save scaler
    scaler = StandardScaler()
    X_train_scaled = scaler.fit_transform(X_train)
    X_test_scaled = scaler.transform(X_test)
    
    # Handle class imbalance with SMOTE
    print("⚖️ Balancing classes with SMOTE...")
    smote = SMOTE(random_state=42)
    X_train_balanced, y_train_balanced = smote.fit_resample(X_train_scaled, y_train)
    
    # Create the experiment if it doesn't exist
    mlflow.set_experiment(MLFLOW_EXPERIMENT_NAME)
    
    with mlflow.start_run(run_name=f"fraud_model_{datetime.now().strftime('%Y%m%d_%H%M%S')}") as run:
        run_id = run.info.run_id
        
        # Log parameters
        mlflow.log_param("model_type", "RandomForest")
        mlflow.log_param("train_size", len(X_train))
        
        # Train model
        print("🌲 Training Random Forest...")
        model = RandomForestClassifier(
            n_estimators=100,
            max_depth=10,
            min_samples_split=10,
            class_weight='balanced',
            random_state=42,
            n_jobs=-1
        )
        model.fit(X_train_balanced, y_train_balanced)
        
        # Make predictions
        y_pred_proba = model.predict_proba(X_test_scaled)[:, 1]
        
        # Calculate metrics
        roc_auc = roc_auc_score(y_test, y_pred_proba)
        mlflow.log_metric("roc_auc", roc_auc)
        
        print(f"🎯 Model Performance: ROC-AUC Score: {roc_auc:.4f}")
        
        # Log model to MLflow
        print("💾 Logging model to MLflow...")
        mlflow.sklearn.log_model(
            sk_model=model, 
            artifact_path="model", 
            registered_model_name=MLFLOW_MODEL_NAME,
            signature=mlflow.models.infer_signature(X_test, y_pred_proba)
        )
        
        # Save and log scaler locally (used by the inference endpoint)
        scaler_path = f'/tmp/scaler_{run_id}.pkl'
        joblib.dump(scaler, scaler_path)
        mlflow.log_artifact(scaler_path, "preprocessing_scaler")

        # Push necessary info to XCom
        context['task_instance'].xcom_push(key='run_id', value=run_id)
        context['task_instance'].xcom_push(key='roc_auc', value=roc_auc)
        context['task_instance'].xcom_push(key='model_uri', value=f"runs:/{run_id}/model")
        context['task_instance'].xcom_push(key='model_name', value=MLFLOW_MODEL_NAME)
        
        print(f"\n✅ MLflow Run ID: {run_id}")
        print(f"📊 MLflow Model URI: runs:/{run_id}/model")

def evaluate_and_register_model(**context):
    """
    Evaluates the trained model against a threshold and registers it in the MLflow Model Registry.
    """
    print("📈 Evaluating and registering model...")
    
    # Pull data from XCom
    roc_auc = context['task_instance'].xcom_pull(task_ids='train_model', key='roc_auc')
    model_uri = context['task_instance'].xcom_pull(task_ids='train_model', key='model_uri')
    model_name = context['task_instance'].xcom_pull(task_ids='train_model', key='model_name')
    
    # Define production threshold
    PRODUCTION_THRESHOLD = 0.75
    
    print(f"Current ROC-AUC: {roc_auc:.4f}, Production Threshold: {PRODUCTION_THRESHOLD}")
    
    client = MlflowClient()
    
    if roc_auc >= PRODUCTION_THRESHOLD:
        print("✅ Model meets performance threshold. Registering to MLflow Model Registry...")
        
        # Create a new version in the Model Registry
        mv = client.create_model_version(
            name=model_name,
            source=model_uri,
            run_id=context['task_instance'].xcom_pull(task_ids='train_model', key='run_id')
        )
        
        # Wait for model to become ready
        while client.get_model_version(model_name, mv.version).status != "READY":
            print(f"Waiting for model version {mv.version} to be registered...")
            time.sleep(5)
            
        # Transition the newly registered model to 'Staging'
        client.transition_model_version_stage(
            name=model_name,
            version=mv.version,
            stage="Staging",
            archive_existing_versions=True
        )
        
        print(f"🎉 Model Version {mv.version} transitioned to **Staging** and is ready for deployment/A/B testing.")
        
        context['task_instance'].xcom_push(key='registered_version', value=mv.version)
        context['task_instance'].xcom_push(key='model_stage', value='Staging')
        
    else:
        print(f"⚠️  WARNING: ROC-AUC ({roc_auc:.4f}) is below the threshold ({PRODUCTION_THRESHOLD}). Model will not be registered.")
        context['task_instance'].xcom_push(key='model_stage', value='Rejected')

# Default arguments
default_args = {
    'owner': 'data-science-team',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Create DAG
with DAG(
    'fraud_detection_pipeline_with_mlops',
    default_args=default_args,
    description='End-to-end fraud detection ML pipeline integrated with MLflow Registry',
    schedule_interval=timedelta(days=7), # Run weekly
    # FIX: Use an explicit datetime object instead of the removed days_ago() function
    start_date=datetime(2024, 1, 1), 
    catchup=False,
    tags=['fraud-detection', 'mlops', 'kafka'],
) as dag:
    
    # Task 1: Download/Generate data
    download_task = PythonOperator(
        task_id='download_data',
        python_callable=download_sample_data,
        do_xcom_push=True,
    )
    
    # Task 2: Preprocess data
    preprocess_task = PythonOperator(
        task_id='preprocess_data',
        python_callable=preprocess_data,
        do_xcom_push=True,
    )
    
    # Task 3: Train model and log to MLflow tracking server
    train_task = PythonOperator(
        task_id='train_model',
        python_callable=train_model,
        do_xcom_push=True,
    )
    
    # Task 4: Evaluate model and register best version to Model Registry
    register_task = PythonOperator(
        task_id='evaluate_and_register_model',
        python_callable=evaluate_and_register_model,
        do_xcom_push=True,
    )
    
    # Task 5: Success notification
    success_task = BashOperator(
        task_id='pipeline_success',
        bash_command=f'echo "✅ Pipeline complete. Model stage: {{ task_instance.xcom_pull(task_ids="evaluate_and_register_model", key="model_stage") }}"',
    )
    
    # Define task dependencies
    download_task >> preprocess_task >> train_task >> register_task >> success_task