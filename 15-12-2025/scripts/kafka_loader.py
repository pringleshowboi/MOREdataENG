import time
import json
import io
from minio import Minio
from minio.error import S3Error
from kafka import KafkaConsumer
from kafka.errors import KafkaError
from datetime import datetime, timezone

# --- Configuration --- #
KAFKA_BROKER = 'localhost:9092'
MINIO_ENDPOINT = 'localhost:9000'
MINIO_ACCESS_KEY = 'minio'
MINIO_SECRET_KEY = 'minio123'
BUCKET_NAME = 'security-data-lake'
TOPICS = ['raw_firewall_logs', 'raw_cloud_logs']

# Buffer settings
MAX_BUFFER_SIZE = 1000  # Number of messages to buffer before writing a file
MAX_BUFFER_TIME = 30  # Seconds to wait before flushing buffer even if not full
buffer = {topic: [] for topic in TOPICS}
last_flush_time = {topic: time.time() for topic in TOPICS}

# --- MinIO Setup --- #
def get_minio_client():
    """Initializes and returns the MinIO client."""
    max_retries = 5
    for attempt in range(max_retries):
        try:
            client = Minio(
                MINIO_ENDPOINT,
                access_key=MINIO_ACCESS_KEY,
                secret_key=MINIO_SECRET_KEY,
                secure=False
            )
            # Test connection
            client.list_buckets()
            print(f"✓ MinIO client connected to {MINIO_ENDPOINT}")
            return client
        except Exception as e:
            if attempt < max_retries - 1:
                print(f"Attempt {attempt + 1}/{max_retries}: MinIO connection failed, retrying in 2s...")
                time.sleep(2)
            else:
                print(f"✗ Error connecting to MinIO after {max_retries} attempts: {e}")
                return None

# --- Kafka Consumer Setup --- #
def get_kafka_consumer():
    """Initializes and returns the Kafka consumer."""
    max_retries = 5
    for attempt in range(max_retries):
        try:
            consumer = KafkaConsumer(
                *TOPICS,
                bootstrap_servers=[KAFKA_BROKER],
                group_id='minio-loader-group',
                auto_offset_reset='earliest',
                enable_auto_commit=False,  # Manual commit for better control
                value_deserializer=lambda m: json.loads(m.decode('utf-8'))
            )
            print(f"✓ Kafka consumer started, listening on: {TOPICS}")
            return consumer
        except KafkaError as e:
            if attempt < max_retries - 1:
                print(f"Attempt {attempt + 1}/{max_retries}: Kafka connection failed, retrying in 2s...")
                time.sleep(2)
            else:
                print(f"✗ Error connecting to Kafka after {max_retries} attempts: {e}")
                return None

# --- Bucket Initialization --- #
def ensure_bucket_exists(minio_client):
    """Ensures the MinIO bucket exists, creates it if not."""
    try:
        if not minio_client.bucket_exists(BUCKET_NAME):
            minio_client.make_bucket(BUCKET_NAME)
            print(f"✓ Created bucket: {BUCKET_NAME}")
        else:
            print(f"✓ Bucket exists: {BUCKET_NAME}")
        return True
    except Exception as e:
        print(f"✗ Failed to check/create bucket: {e}")
        return False

# --- Data Writing Function --- #
def write_to_minio(minio_client, topic, data_list):
    """Writes a batch of messages to a MinIO file, partitioned by time."""
    if not data_list:
        return False

    # Use the current time for partitioning
    now_utc = datetime.now(timezone.utc)
    
    # Partitioning: <topic>/year=YYYY/month=MM/day=DD/file_timestamp.json
    partition_path = (
        f"{topic}/year={now_utc.strftime('%Y')}/month={now_utc.strftime('%m')}/day={now_utc.strftime('%d')}/"
    )
    
    # Unique filename based on timestamp
    object_name = partition_path + f"{now_utc.strftime('%Y%m%d_%H%M%S_%f')}.json"
    
    # Combine all messages into JSONL format (one JSON object per line)
    content = "\n".join([json.dumps(msg) for msg in data_list])
    
    # Convert to bytes and create file-like object
    data_bytes = content.encode('utf-8')
    data_stream = io.BytesIO(data_bytes)
    data_len = len(data_bytes)

    try:
        minio_client.put_object(
            BUCKET_NAME,
            object_name,
            data_stream,
            data_len,
            content_type='application/json'
        )
        print(f"✓ Uploaded {len(data_list)} messages to MinIO: {object_name}")
        return True
    except S3Error as e:
        print(f"✗ MinIO S3 Error: {e}")
        return False
    except Exception as e:
        print(f"✗ General Error during MinIO upload: {e}")
        return False


# --- Main Logic --- #
def run_loader():
    """Main function to run the Kafka consumer and MinIO loader."""
    print("=" * 60)
    print("Starting Kafka to MinIO Data Loader")
    print("=" * 60)
    
    # Initialize MinIO client
    minio_client = get_minio_client()
    if not minio_client:
        print("Failed to initialize MinIO client. Exiting.")
        return

    # Ensure bucket exists
    if not ensure_bucket_exists(minio_client):
        print("Failed to ensure bucket exists. Exiting.")
        return

    # Initialize Kafka consumer
    consumer = get_kafka_consumer()
    if not consumer:
        print("Failed to initialize Kafka consumer. Exiting.")
        return

    print("\n" + "=" * 60)
    print("Data loader is running. Press Ctrl+C to stop.")
    print("=" * 60 + "\n")

    message_count = 0
    
    try:
        while True:
            try:
                # Poll for messages with a timeout
                messages = consumer.poll(timeout_ms=1000)
                
                # Process received messages
                if messages:
                    for tp, records in messages.items():
                        topic = tp.topic
                        for record in records:
                            # Append message to the topic's buffer
                            buffer[topic].append(record.value)
                            message_count += 1
                            
                            # Check buffer size and write to MinIO if threshold is reached
                            if len(buffer[topic]) >= MAX_BUFFER_SIZE:
                                if write_to_minio(minio_client, topic, buffer[topic]):
                                    buffer[topic].clear()
                                    last_flush_time[topic] = time.time()
                                else:
                                    print(f"Failed to write buffer for {topic}, will retry")

                    # Commit offsets after processing
                    consumer.commit()
                
                # Check for time-based buffer flush
                current_time = time.time()
                for topic in TOPICS:
                    if buffer[topic] and (current_time - last_flush_time[topic]) >= MAX_BUFFER_TIME:
                        print(f"⏰ Time-based flush for {topic} ({len(buffer[topic])} messages)")
                        if write_to_minio(minio_client, topic, buffer[topic]):
                            buffer[topic].clear()
                            last_flush_time[topic] = current_time
                
                # Small sleep to prevent CPU spinning
                if not messages:
                    time.sleep(0.1)
                    
            except KafkaError as e:
                print(f"✗ Kafka error in consumer loop: {e}")
                time.sleep(5)
            except Exception as e:
                print(f"✗ Unexpected error in consumer loop: {e}")
                time.sleep(5)
                
    except KeyboardInterrupt:
        print("\n\n" + "=" * 60)
        print("Shutting down gracefully...")
        print("=" * 60)
        
        # Flush any remaining buffers
        for topic in TOPICS:
            if buffer[topic]:
                print(f"Flushing remaining {len(buffer[topic])} messages from {topic}")
                write_to_minio(minio_client, topic, buffer[topic])
        
        # Close consumer
        consumer.close()
        print(f"\n✓ Processed {message_count} total messages")
        print("✓ Data loader stopped cleanly")

if __name__ == "__main__":
    run_loader()