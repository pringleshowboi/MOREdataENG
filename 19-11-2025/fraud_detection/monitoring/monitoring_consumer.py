"""
Monitoring Consumer Service

This service reads scored transaction data from the 'scored_transactions'
Kafka topic and stores it in a PostgreSQL database for real-time monitoring
and visualization via tools like PowerBI or Metabase.
"""

import os
import json
import logging
import time
from datetime import datetime
from typing import Dict, Any

from kafka import KafkaConsumer
import psycopg2
from psycopg2 import sql, extras

# --- Configuration ---
LOGGING_LEVEL = os.getenv('LOG_LEVEL', 'INFO')
logging.basicConfig(
    level=getattr(logging, LOGGING_LEVEL.upper()),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Kafka Configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:29092')
KAFKA_TOPIC = os.getenv('KAFKA_OUTPUT_TOPIC', 'scored_transactions')
CONSUMER_GROUP = os.getenv('KAFKA_MONITORING_GROUP', 'fraud-monitoring-group')

# PostgreSQL Configuration
POSTGRES_DB = os.getenv('POSTGRES_DB', 'mlops')
POSTGRES_USER = os.getenv('POSTGRES_USER', 'airflow')
POSTGRES_PASSWORD = os.getenv('POSTGRES_PASSWORD', 'airflow')
POSTGRES_HOST = os.getenv('POSTGRES_HOST', 'postgres')
POSTGRES_PORT = os.getenv('POSTGRES_PORT', '5432')
DB_TABLE = os.getenv('DB_TABLE', 'real_time_scores')


class DatabaseManager:
    """Manages connections and insertions to the PostgreSQL database."""
    def __init__(self):
        self.conn = None

    def connect(self):
        """Establishes a connection to the PostgreSQL database."""
        if self.conn and not self.conn.closed:
            return self.conn
            
        try:
            logger.info("Attempting to connect to PostgreSQL...")
            self.conn = psycopg2.connect(
                dbname=POSTGRES_DB,
                user=POSTGRES_USER,
                password=POSTGRES_PASSWORD,
                host=POSTGRES_HOST,
                port=POSTGRES_PORT,
                connect_timeout=10
            )
            self.conn.autocommit = False # Use manual transactions for bulk insert
            logger.info("Successfully connected to PostgreSQL")
            return self.conn
        except Exception as e:
            logger.error(f"Failed to connect to PostgreSQL: {e}")
            self.conn = None
            raise

    def close(self):
        """Closes the database connection."""
        if self.conn:
            self.conn.close()
            logger.info("PostgreSQL connection closed.")

    def initialize_table(self):
        """Creates the necessary table if it doesn't exist."""
        try:
            conn = self.connect()
            with conn.cursor() as cur:
                create_table_query = sql.SQL("""
                    CREATE TABLE IF NOT EXISTS {} (
                        id SERIAL PRIMARY KEY,
                        transaction_id VARCHAR(255) UNIQUE NOT NULL,
                        user_id VARCHAR(255),
                        amount FLOAT,
                        location VARCHAR(10),
                        is_simulated_fraud BOOLEAN,
                        prediction INTEGER,
                        fraud_probability FLOAT,
                        is_fraud BOOLEAN,
                        model_version VARCHAR(50),
                        scored_at TIMESTAMP WITHOUT TIME ZONE
                    );
                """).format(sql.Identifier(DB_TABLE))
                cur.execute(create_table_query)
                conn.commit()
            logger.info(f"Table '{DB_TABLE}' checked/created successfully.")
        except Exception as e:
            logger.error(f"Error initializing table: {e}")
            if conn:
                conn.rollback()
            raise

    def insert_scores(self, batch_data: list[Dict[str, Any]]):
        """
        Performs a batch insert of scored transactions into the database.
        
        Args:
            batch_data: A list of scored transaction dictionaries.
        """
        if not batch_data:
            return

        conn = self.connect()
        if not conn:
            return

        # Map Kafka output to database columns
        records = []
        for score in batch_data:
            original = score.get('original_data', {})
            
            # Simple check to prevent duplicates (though transaction_id is UNIQUE)
            if not score.get('transaction_id'):
                logger.warning("Skipping record with missing transaction_id.")
                continue

            records.append((
                score.get('transaction_id'),
                original.get('user_id'),
                original.get('amount'),
                original.get('location'),
                original.get('is_simulated_fraud'), # For verification/backtesting
                score.get('prediction'),
                score.get('fraud_probability'),
                score.get('is_fraud'),
                score.get('model_version'),
                datetime.fromisoformat(score['scored_at'])
            ))

        insert_query = sql.SQL("""
            INSERT INTO {} (
                transaction_id, user_id, amount, location, is_simulated_fraud, 
                prediction, fraud_probability, is_fraud, model_version, scored_at
            ) VALUES %s
            ON CONFLICT (transaction_id) DO NOTHING;
        """).format(sql.Identifier(DB_TABLE))

        try:
            with conn.cursor() as cur:
                # Use execute_values for efficient batch insertion
                extras.execute_values(
                    cur,
                    insert_query,
                    records,
                    page_size=len(records)
                )
            conn.commit()
            logger.info(f"Successfully inserted {len(records)} records.")
        except Exception as e:
            logger.error(f"Batch insert failed: {e}")
            conn.rollback()


class MonitoringConsumer:
    """Consumes scored data and persists it for BI."""
    
    def __init__(self):
        self.db_manager = DatabaseManager()
        self.consumer = None
        self.messages_processed = 0
        self.batch_size = int(os.getenv('DB_BATCH_SIZE', '50'))

    def initialize(self) -> bool:
        """Initialize DB connection, table, and Kafka consumer."""
        logger.info("Initializing Monitoring Consumer Service...")

        # Initialize DB and table (with retries)
        max_retries = 5
        for attempt in range(max_retries):
            try:
                self.db_manager.connect()
                self.db_manager.initialize_table()
                break
            except Exception:
                logger.warning(f"DB initialization attempt {attempt + 1}/{max_retries} failed, retrying in 5s...")
                time.sleep(5)
        else:
            logger.critical("Failed to initialize database after maximum retries.")
            return False

        # Initialize Kafka consumer
        try:
            self.consumer = KafkaConsumer(
                KAFKA_TOPIC,
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                group_id=CONSUMER_GROUP,
                auto_offset_reset='latest',
                enable_auto_commit=True,
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                max_poll_records=self.batch_size,
                consumer_timeout_ms=1000 # Wait for 1 second for messages
            )
            logger.info(f"Kafka consumer connected to {KAFKA_TOPIC}")
        except Exception as e:
            logger.error(f"Failed to initialize Kafka consumer: {e}")
            return False

        logger.info("Monitoring Consumer Service initialized successfully.")
        return True

    def run(self):
        """Main service loop to consume and persist data."""
        logger.info("Starting Monitoring Consumer...")
        
        try:
            while True:
                # Consumer poll returns a dictionary {TopicPartition: [messages]}
                message_batches = self.consumer.poll()
                
                if not message_batches:
                    time.sleep(1) # Sleep briefly if no messages were found
                    continue

                records_to_insert = []
                for tp, messages in message_batches.items():
                    for message in messages:
                        try:
                            records_to_insert.append(message.value)
                            self.messages_processed += 1
                        except Exception as e:
                            logger.error(f"Error parsing message value: {e}", exc_info=True)
                
                # Insert the collected batch into PostgreSQL
                if records_to_insert:
                    self.db_manager.insert_scores(records_to_insert)
                
                # Log periodically
                if self.messages_processed % (self.batch_size * 10) == 0 and self.messages_processed > 0:
                    logger.info(f"Total messages processed and stored: {self.messages_processed}")


        except KeyboardInterrupt:
            logger.info("Service interrupted by user")
        except Exception as e:
            logger.error(f"Fatal error in service loop: {e}", exc_info=True)
        finally:
            self.shutdown()

    def shutdown(self):
        """Clean shutdown of all components."""
        logger.info("Shutting down Monitoring Consumer Service...")
        
        if self.consumer:
            self.consumer.close()
            logger.info("Kafka consumer closed")
        
        self.db_manager.close()
        
        logger.info(f"Final statistics: Processed: {self.messages_processed}")


if __name__ == "__main__":
    # Ensure necessary environment variables are set or defaults are used
    service = MonitoringConsumer()
    
    if service.initialize():
        service.run()
    else:
        logger.critical("Service failed to initialize, exiting.")