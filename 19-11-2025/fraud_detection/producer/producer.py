import os
import time
import json
import logging
import random
from confluent_kafka import Producer, KafkaException
from faker import Faker

# --- Configuration ---
KAFKA_BROKER = os.getenv('KAFKA_BOOTSTRAP_SERVERS', os.getenv('KAFKA_BROKER', 'kafka:29092'))
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'raw_transactions')
LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL),
    format='%(asctime)s | %(levelname)s | %(message)s'
)

# Initialize Faker for synthetic data generation
fake = Faker()

def generate_transaction():
    """Generates a synthetic transaction record with realistic fraud patterns."""
    user_id = fake.uuid4()
    card_type = random.choice(['visa', 'mastercard', 'amex', 'discover'])
    
    # Determine if this is a fraudulent transaction (5% chance)
    is_fraud = random.random() < 0.05
    
    if is_fraud:
        # Fraudulent transactions: higher amounts and unusual patterns
        amount = round(random.uniform(1000.0, 10000.0), 2)
        # Fraud often happens in foreign countries
        location = random.choice(['NG', 'RU', 'CN', 'BR', 'IN'])
        # Unusual times (late night/early morning)
        hour_offset = random.randint(0, 6)  # 12 AM to 6 AM
    else:
        # Normal transactions: typical spending amounts
        amount = round(random.uniform(5.0, 500.0), 2)
        # Common locations
        location = random.choice(['US', 'GB', 'CA', 'DE', 'FR', 'AU', 'JP'])
        # Normal business hours
        hour_offset = random.randint(8, 20)  # 8 AM to 8 PM

    transaction = {
        'transaction_id': fake.uuid4(),
        'user_id': user_id,
        'timestamp': int(time.time() * 1000),  # Milliseconds since epoch
        'amount': amount,
        'currency': 'USD',
        'location': location,
        'card_type': card_type,
        'merchant_category': random.choice([
            'retail', 'grocery', 'restaurant', 'gas_station', 
            'online', 'travel', 'entertainment'
        ]),
        'is_simulated_fraud': is_fraud
    }
    
    # Log fraud cases for monitoring
    if is_fraud:
        logging.warning(
            f"🚨 FRAUD SIMULATED | ID: {transaction['transaction_id']} | "
            f"Amount: ${amount:,.2f} | Location: {location}"
        )
    
    return transaction

def delivery_report(err, msg):
    """Called once for each message produced to indicate success or failure."""
    if err is not None:
        logging.error(f"❌ Message delivery failed: {err}")
    else:
        logging.debug(
            f"✓ Message delivered to {msg.topic()} "
            f"[{msg.partition()}] at offset {msg.offset()}"
        )

def initialize_producer():
    """Initializes and returns the Kafka Producer with retry logic."""
    max_retries = 10
    retry_count = 0
    
    while retry_count < max_retries:
        try:
            logging.info(f"Attempting to connect to Kafka at {KAFKA_BROKER}...")
            producer_config = {
                'bootstrap.servers': KAFKA_BROKER,
                'client.id': 'fraud-transaction-producer',
                'acks': 'all',  # Wait for all replicas
                'retries': 3,
                'compression.type': 'snappy'
            }
            
            producer = Producer(producer_config)
            
            # Test the connection
            producer.poll(timeout=5)
            
            logging.info("✓ Kafka Producer initialized successfully.")
            return producer
            
        except KafkaException as e:
            retry_count += 1
            logging.error(
                f"Kafka initialization failed (attempt {retry_count}/{max_retries}): {e}"
            )
            if retry_count < max_retries:
                time.sleep(min(5 * retry_count, 30))  # Exponential backoff
            else:
                logging.critical("Max retries reached. Exiting.")
                raise
                
        except Exception as e:
            retry_count += 1
            logging.error(
                f"Unexpected error during producer setup (attempt {retry_count}/{max_retries}): {e}"
            )
            if retry_count < max_retries:
                time.sleep(min(5 * retry_count, 30))
            else:
                logging.critical("Max retries reached. Exiting.")
                raise

def produce_transactions(producer):
    """Continuously generates and sends transactions to Kafka."""
    logging.info(f"🚀 Starting continuous transaction production to topic: {KAFKA_TOPIC}")
    
    transaction_count = 0
    fraud_count = 0
    last_stats_time = time.time()
    
    while True:
        try:
            # Generate transaction
            transaction = generate_transaction()
            transaction_data = json.dumps(transaction).encode('utf-8')
            
            # Poll for delivery reports
            producer.poll(0)
            
            # Produce message
            producer.produce(
                KAFKA_TOPIC,
                key=transaction['transaction_id'].encode('utf-8'),
                value=transaction_data,
                callback=delivery_report
            )
            
            # Update counters
            transaction_count += 1
            if transaction['is_simulated_fraud']:
                fraud_count += 1
            
            # Flush every 100 messages or every 10 seconds
            if transaction_count % 100 == 0:
                producer.flush()
            
            # Print statistics every 60 seconds
            if time.time() - last_stats_time >= 60:
                fraud_rate = (fraud_count / transaction_count * 100) if transaction_count > 0 else 0
                logging.info(
                    f"📊 Stats: {transaction_count} transactions produced | "
                    f"{fraud_count} fraudulent ({fraud_rate:.1f}%)"
                )
                last_stats_time = time.time()
            
            # Small delay between transactions (2-5 per second)
            time.sleep(random.uniform(0.2, 0.5))
            
        except BufferError:
            # Producer queue is full - wait and try again
            logging.warning("Producer queue full, waiting...")
            producer.flush()
            time.sleep(1)
            
        except KeyboardInterrupt:
            logging.info("Shutting down producer...")
            producer.flush()
            break
            
        except Exception as e:
            logging.error(f"Error producing transaction: {e}", exc_info=True)
            time.sleep(5)

if __name__ == "__main__":
    try:
        producer_instance = initialize_producer()
        produce_transactions(producer_instance)
    except KeyboardInterrupt:
        logging.info("Producer stopped by user")
    except Exception as e:
        logging.critical(f"Fatal error: {e}", exc_info=True)
        exit(1)