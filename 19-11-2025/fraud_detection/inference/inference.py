"""
Real-Time Fraud Detection Inference Service

This service consumes raw transaction data from Kafka, loads the latest
Staging model from MLflow, performs inference, and produces scored results
back to Kafka.
"""

import os
import json
import logging
import pickle
import time
from typing import Dict, Any, Optional
from datetime import datetime

import numpy as np
import pandas as pd
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import KafkaError
import mlflow
from mlflow.tracking import MlflowClient

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class MLflowModelLoader:
    """Handles loading models and artifacts from MLflow."""
    
    def __init__(self, tracking_uri: str, model_name: str, model_stage: str = "Staging"):
        """
        Initialize the MLflow model loader.
        
        Args:
            tracking_uri: MLflow tracking server URI
            model_name: Name of the registered model
            model_stage: Model stage to load (Production, Staging, etc.)
        """
        self.tracking_uri = tracking_uri
        self.model_name = model_name
        self.model_stage = model_stage
        self.client = MlflowClient(tracking_uri=tracking_uri)
        mlflow.set_tracking_uri(tracking_uri)
        
        self.model = None
        self.scaler = None
        self.feature_columns = None
        self.model_version = None
        self.last_load_time = None
        
    def load_model(self) -> bool:
        """
        Load the model and associated artifacts from MLflow.
        
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            logger.info(f"Loading model '{self.model_name}' from stage '{self.model_stage}'...")
            
            # Get the latest version in the specified stage
            versions = self.client.get_latest_versions(self.model_name, stages=[self.model_stage])
            
            if not versions:
                logger.error(f"No model found in stage '{self.model_stage}' for '{self.model_name}'")
                return False
            
            model_version = versions[0]
            self.model_version = model_version.version
            model_uri = f"models:/{self.model_name}/{self.model_stage}"
            
            logger.info(f"Loading model version {self.model_version} from {model_uri}")
            
            # Load the model
            self.model = mlflow.sklearn.load_model(model_uri)
            
            # Get run_id to load additional artifacts
            run_id = model_version.run_id
            
            # Download and load scaler
            try:
                scaler_path = mlflow.artifacts.download_artifacts(
                    run_id=run_id,
                    artifact_path="scaler.pkl"
                )
                with open(scaler_path, 'rb') as f:
                    self.scaler = pickle.load(f)
                logger.info("Scaler loaded successfully")
            except Exception as e:
                logger.warning(f"Could not load scaler: {e}")
                self.scaler = None
            
            # Download and load feature columns
            try:
                features_path = mlflow.artifacts.download_artifacts(
                    run_id=run_id,
                    artifact_path="feature_columns.json"
                )
                with open(features_path, 'r') as f:
                    self.feature_columns = json.load(f)
                logger.info(f"Feature columns loaded: {len(self.feature_columns)} features")
            except Exception as e:
                logger.warning(f"Could not load feature columns: {e}")
                self.feature_columns = None
            
            self.last_load_time = datetime.now()
            logger.info(f"Model version {self.model_version} loaded successfully at {self.last_load_time}")
            return True
            
        except Exception as e:
            logger.error(f"Error loading model: {e}", exc_info=True)
            return False
    
    def reload_if_needed(self, check_interval_seconds: int = 300) -> bool:
        """
        Check if a new model version is available and reload if necessary.
        
        Args:
            check_interval_seconds: How often to check for updates
            
        Returns:
            bool: True if model was reloaded
        """
        if self.last_load_time is None:
            return self.load_model()
        
        time_since_load = (datetime.now() - self.last_load_time).total_seconds()
        if time_since_load < check_interval_seconds:
            return False
        
        try:
            versions = self.client.get_latest_versions(self.model_name, stages=[self.model_stage])
            if versions and versions[0].version != self.model_version:
                logger.info(f"New model version detected: {versions[0].version} (current: {self.model_version})")
                return self.load_model()
        except Exception as e:
            logger.error(f"Error checking for model updates: {e}")
        
        return False


class FraudInferenceService:
    """Main inference service for real-time fraud detection."""
    
    def __init__(self):
        """Initialize the inference service with configuration from environment."""
        # Kafka configuration
        self.kafka_bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:29092')
        self.input_topic = os.getenv('KAFKA_INPUT_TOPIC', 'raw_transactions')
        self.output_topic = os.getenv('KAFKA_OUTPUT_TOPIC', 'scored_transactions')
        self.consumer_group = os.getenv('KAFKA_CONSUMER_GROUP', 'fraud-inference-group')
        
        # MLflow configuration
        self.mlflow_uri = os.getenv('MLFLOW_TRACKING_URI', 'http://mlflow-server:5000')
        self.model_name = os.getenv('MODEL_NAME', 'fraud_detection_model')
        self.model_stage = os.getenv('MODEL_STAGE', 'Staging')
        
        # Service configuration
        self.model_reload_interval = int(os.getenv('MODEL_RELOAD_INTERVAL', '300'))  # 5 minutes
        self.batch_size = int(os.getenv('BATCH_SIZE', '1'))  # Process one message at a time by default
        
        # Initialize components
        self.model_loader = None
        self.consumer = None
        self.producer = None
        self.messages_processed = 0
        self.errors_count = 0
        
    def initialize(self) -> bool:
        """
        Initialize all components (MLflow, Kafka).
        
        Returns:
            bool: True if successful
        """
        logger.info("Initializing Fraud Inference Service...")
        
        # Initialize MLflow model loader
        try:
            self.model_loader = MLflowModelLoader(
                tracking_uri=self.mlflow_uri,
                model_name=self.model_name,
                model_stage=self.model_stage
            )
            
            if not self.model_loader.load_model():
                logger.error("Failed to load initial model")
                return False
                
        except Exception as e:
            logger.error(f"Failed to initialize MLflow: {e}", exc_info=True)
            return False
        
        # Initialize Kafka consumer
        try:
            self.consumer = KafkaConsumer(
                self.input_topic,
                bootstrap_servers=self.kafka_bootstrap_servers,
                group_id=self.consumer_group,
                auto_offset_reset='latest',
                enable_auto_commit=True,
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                max_poll_records=self.batch_size
            )
            logger.info(f"Kafka consumer connected to {self.input_topic}")
        except Exception as e:
            logger.error(f"Failed to initialize Kafka consumer: {e}", exc_info=True)
            return False
        
        # Initialize Kafka producer
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=self.kafka_bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                acks='all',
                retries=3
            )
            logger.info(f"Kafka producer connected, will write to {self.output_topic}")
        except Exception as e:
            logger.error(f"Failed to initialize Kafka producer: {e}", exc_info=True)
            return False
        
        logger.info("Fraud Inference Service initialized successfully")
        return True
    
    def preprocess_transaction(self, transaction: Dict[str, Any]) -> Optional[pd.DataFrame]:
        """
        Preprocess a raw transaction for model inference.
        
        Args:
            transaction: Raw transaction data from Kafka
            
        Returns:
            DataFrame ready for model inference, or None if preprocessing fails
        """
        try:
            # Convert to DataFrame
            df = pd.DataFrame([transaction])
            
            # If we have feature columns from MLflow, ensure we have all required features
            if self.model_loader.feature_columns:
                # Add missing columns with default values
                for col in self.model_loader.feature_columns:
                    if col not in df.columns:
                        df[col] = 0
                
                # Select only the required features in the correct order
                df = df[self.model_loader.feature_columns]
            
            # Apply scaling if scaler is available
            if self.model_loader.scaler:
                df_scaled = self.model_loader.scaler.transform(df)
                df = pd.DataFrame(df_scaled, columns=df.columns)
            
            return df
            
        except Exception as e:
            logger.error(f"Error preprocessing transaction: {e}", exc_info=True)
            return None
    
    def score_transaction(self, transaction: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Score a single transaction using the loaded model.
        
        Args:
            transaction: Raw transaction data
            
        Returns:
            Scored transaction with prediction and probability, or None if scoring fails
        """
        try:
            # Store original transaction data
            transaction_id = transaction.get('transaction_id', 'unknown')
            
            # Preprocess
            features = self.preprocess_transaction(transaction)
            if features is None:
                return None
            
            # Make prediction
            prediction = self.model_loader.model.predict(features)[0]
            
            # Get prediction probability if available
            try:
                prediction_proba = self.model_loader.model.predict_proba(features)[0]
                fraud_probability = float(prediction_proba[1])  # Probability of fraud (class 1)
            except Exception:
                fraud_probability = float(prediction)  # Fallback to binary prediction
            
            # Create scored result
            scored_transaction = {
                'transaction_id': transaction_id,
                'original_data': transaction,
                'prediction': int(prediction),
                'fraud_probability': fraud_probability,
                'is_fraud': bool(prediction == 1),
                'model_version': self.model_loader.model_version,
                'scored_at': datetime.now().isoformat(),
                'scoring_time_ms': 0  # Could add timing if needed
            }
            
            return scored_transaction
            
        except Exception as e:
            logger.error(f"Error scoring transaction: {e}", exc_info=True)
            return None
    
    def process_message(self, message) -> bool:
        """
        Process a single Kafka message.
        
        Args:
            message: Kafka message containing transaction data
            
        Returns:
            bool: True if processed successfully
        """
        try:
            transaction = message.value
            
            # Score the transaction
            scored = self.score_transaction(transaction)
            
            if scored is None:
                logger.error(f"Failed to score transaction")
                self.errors_count += 1
                return False
            
            # Send to output topic
            future = self.producer.send(self.output_topic, value=scored)
            future.get(timeout=10)  # Wait for confirmation
            
            self.messages_processed += 1
            
            # Log periodically
            if self.messages_processed % 100 == 0:
                logger.info(
                    f"Processed {self.messages_processed} messages "
                    f"(Errors: {self.errors_count}, "
                    f"Model version: {self.model_loader.model_version})"
                )
            
            # Log fraud detections
            if scored['is_fraud']:
                logger.warning(
                    f"FRAUD DETECTED: Transaction {scored['transaction_id']} "
                    f"(Probability: {scored['fraud_probability']:.3f})"
                )
            
            return True
            
        except Exception as e:
            logger.error(f"Error processing message: {e}", exc_info=True)
            self.errors_count += 1
            return False
    
    def run(self):
        """Main service loop."""
        logger.info("Starting Fraud Inference Service...")
        logger.info(f"Consuming from: {self.input_topic}")
        logger.info(f"Publishing to: {self.output_topic}")
        logger.info(f"Model: {self.model_name} ({self.model_stage})")
        
        try:
            for message in self.consumer:
                # Check if model needs reloading
                self.model_loader.reload_if_needed(self.model_reload_interval)
                
                # Process the message
                self.process_message(message)
                
        except KeyboardInterrupt:
            logger.info("Service interrupted by user")
        except Exception as e:
            logger.error(f"Fatal error in service loop: {e}", exc_info=True)
        finally:
            self.shutdown()
    
    def shutdown(self):
        """Clean shutdown of all components."""
        logger.info("Shutting down Fraud Inference Service...")
        
        if self.consumer:
            self.consumer.close()
            logger.info("Kafka consumer closed")
        
        if self.producer:
            self.producer.flush()
            self.producer.close()
            logger.info("Kafka producer closed")
        
        logger.info(
            f"Final statistics: "
            f"Processed: {self.messages_processed}, "
            f"Errors: {self.errors_count}"
        )


def main():
    """Entry point for the service."""
    service = FraudInferenceService()
    
    # Wait for dependencies to be ready
    logger.info("Waiting for dependencies...")
    time.sleep(10)  # Give Kafka and MLflow time to be fully ready
    
    # Initialize
    max_retries = 5
    for attempt in range(max_retries):
        if service.initialize():
            break
        logger.warning(f"Initialization attempt {attempt + 1}/{max_retries} failed, retrying in 10s...")
        time.sleep(10)
    else:
        logger.error("Failed to initialize after maximum retries")
        return
    
    # Run the service
    service.run()


if __name__ == "__main__":
    main()