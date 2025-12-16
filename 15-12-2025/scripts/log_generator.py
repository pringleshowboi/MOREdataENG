import time
import json
import random
from faker import Faker
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

# --- Configuration ---#
KAFKA_BROKER = 'localhost:9092'  # FIXED: Changed from 127.0.0.1
TOPIC_FIREWALL = 'raw_firewall_logs'
TOPIC_CLOUD = 'raw_cloud_logs'

# --- Initialize Faker ---#
fake = Faker()

def generate_firewall_log():
    """Generate a log with a non-standard, network centric schema."""
    action = random.choice(['ALLOW', 'DENY', 'DROP', 'RESET'])
    if random.random() < 0.1:
        action = 'DROP'
    
    return {
        "ts_unix": int(time.time() * 1000),
        "src_ip": fake.ipv4_public(),
        "dest_ip": fake.ipv4_public(),
        "src_port": random.randint(1024, 65535),
        "dest_port": random.randint(1, 65535),
        "protocol": random.choice(['TCP', 'UDP', 'ICMP']),
        "act": action,
        "bytes_sent": random.randint(40, 1500),
    }

def generate_cloud_audit_log():
    """Generates a log with a non-standard, activity-centric schema."""
    operation = random.choice([
        's3:PutObject', 'ec2:RunInstances', 'iam:ChangePassword', 
        's3:GetObject', 's3:DeleteObject', 'iam:Login'
    ])
    
    actor = fake.email() if random.random() < 0.3 else fake.user_name()

    return {
        "event_time_iso": fake.iso8601(),
        "user_id": actor,
        "resource_name": random.choice(['S3_Bucket_A', 'EC2_Server_B', 'IAM_Role_C']),
        "operation": operation,
        "success": True if 'GetObject' in operation or 'Login' in operation else random.choice([True, False]),
        "source_service": "CloudTrail"
    }

def run_producer():
    # Add retry logic for initial connection
    max_retries = 5
    for attempt in range(max_retries):
        try:
            producer = KafkaProducer(
                bootstrap_servers=[KAFKA_BROKER],
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                acks=1,
                retries=3
            )
            print(f"✓ Connected to Kafka at {KAFKA_BROKER}")
            break
        except NoBrokersAvailable:
            if attempt < max_retries - 1:
                print(f"Attempt {attempt + 1}/{max_retries}: Kafka not ready, retrying in 2s...")
                time.sleep(2)
            else:
                print("ERROR: Could not connect to Kafka after multiple attempts")
                return
    
    print(f"Starting Kafka Producer. Sending to topics: {TOPIC_FIREWALL}, {TOPIC_CLOUD}")
    
    try:
        while True:
            if random.choice([True, False]):
                log = generate_firewall_log()
                topic = TOPIC_FIREWALL
            else:
                log = generate_cloud_audit_log()
                topic = TOPIC_CLOUD
            
            try:
                producer.send(topic, value=log)
                action_key = "act" if topic == TOPIC_FIREWALL else "operation"
                print(f"✓ Produced to {topic}: {log.get(action_key)}")
                time.sleep(random.uniform(0.1, 0.5))
            
            except Exception as e:
                print(f"✗ Error producing message: {e}")
                time.sleep(5)
    
    except KeyboardInterrupt:
        print("\n\nShutting down producer...")
    finally:
        producer.close()
        print("Producer closed")

if __name__ == "__main__":
    run_producer()