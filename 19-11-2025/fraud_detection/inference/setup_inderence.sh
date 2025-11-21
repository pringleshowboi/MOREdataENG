#!/bin/bash

# Setup script for the Real-Time Fraud Detection Inference Service
# This script helps set up, test, and monitor the inference service

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Configuration
KAFKA_CONTAINER="kafka"
MLFLOW_URL="http://localhost:5000"
INPUT_TOPIC="raw_transactions"
OUTPUT_TOPIC="scored_transactions"

# Function to print colored messages
print_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

print_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Function to check if a service is running
check_service() {
    local service_name=$1
    if docker-compose ps | grep -q "$service_name.*Up"; then
        print_info "$service_name is running"
        return 0
    else
        print_error "$service_name is not running"
        return 1
    fi
}

# Function to wait for a service to be healthy
wait_for_service() {
    local service_name=$1
    local max_attempts=$2
    local attempt=1
    
    print_info "Waiting for $service_name to be ready..."
    
    while [ $attempt -le $max_attempts ]; do
        if check_service "$service_name" > /dev/null 2>&1; then
            print_info "$service_name is ready!"
            return 0
        fi
        echo -n "."
        sleep 2
        attempt=$((attempt + 1))
    done
    
    print_error "$service_name failed to start within expected time"
    return 1
}

# Function to create Kafka topics if they don't exist
create_topics() {
    print_info "Checking Kafka topics..."
    
    # Check if input topic exists
    if ! docker-compose exec -T $KAFKA_CONTAINER kafka-topics \
        --bootstrap-server localhost:9092 \
        --list 2>/dev/null | grep -q "^${INPUT_TOPIC}$"; then
        print_info "Creating topic: $INPUT_TOPIC"
        docker-compose exec -T $KAFKA_CONTAINER kafka-topics \
            --bootstrap-server localhost:9092 \
            --create \
            --topic $INPUT_TOPIC \
            --partitions 3 \
            --replication-factor 1
    else
        print_info "Topic $INPUT_TOPIC already exists"
    fi
    
    # Check if output topic exists
    if ! docker-compose exec -T $KAFKA_CONTAINER kafka-topics \
        --bootstrap-server localhost:9092 \
        --list 2>/dev/null | grep -q "^${OUTPUT_TOPIC}$"; then
        print_info "Creating topic: $OUTPUT_TOPIC"
        docker-compose exec -T $KAFKA_CONTAINER kafka-topics \
            --bootstrap-server localhost:9092 \
            --create \
            --topic $OUTPUT_TOPIC \
            --partitions 3 \
            --replication-factor 1
    else
        print_info "Topic $OUTPUT_TOPIC already exists"
    fi
}

# Function to check MLflow model availability
check_mlflow_model() {
    print_info "Checking MLflow for models..."
    
    if curl -s "${MLFLOW_URL}/api/2.0/mlflow/registered-models/list" > /dev/null; then
        print_info "MLflow is accessible"
        
        # Try to get model info (basic check)
        local models=$(curl -s "${MLFLOW_URL}/api/2.0/mlflow/registered-models/list" | grep -o '"name":"[^"]*"' | cut -d'"' -f4)
        
        if [ -z "$models" ]; then
            print_warn "No models found in MLflow registry"
            print_warn "Please train and register a model before starting inference"
            return 1
        else
            print_info "Found models: $models"
            return 0
        fi
    else
        print_error "Cannot connect to MLflow at $MLFLOW_URL"
        return 1
    fi
}

# Function to send a test transaction
send_test_transaction() {
    print_info "Sending test transaction..."
    
    local test_transaction='{
  "transaction_id": "test_'$(date +%s)'",
  "amount": 150.50,
  "merchant_category": "retail",
  "transaction_hour": 14,
  "day_of_week": 2,
  "distance_from_home": 5.2,
  "distance_from_last_transaction": 2.1,
  "ratio_to_median_purchase_price": 1.2,
  "repeat_retailer": 1,
  "used_chip": 1,
  "used_pin_number": 1,
  "online_order": 0
}'
    
    echo "$test_transaction" | docker-compose exec -T $KAFKA_CONTAINER \
        kafka-console-producer \
        --bootstrap-server localhost:9092 \
        --topic $INPUT_TOPIC
    
    print_info "Test transaction sent!"
}

# Function to monitor output topic
monitor_output() {
    local duration=${1:-10}
    print_info "Monitoring output topic for $duration seconds..."
    print_info "Press Ctrl+C to stop early"
    
    timeout $duration docker-compose exec $KAFKA_CONTAINER \
        kafka-console-consumer \
        --bootstrap-server localhost:9092 \
        --topic $OUTPUT_TOPIC \
        --from-beginning \
        --max-messages 10 2>/dev/null || true
}

# Function to show consumer group lag
show_consumer_lag() {
    print_info "Consumer group lag information:"
    docker-compose exec -T $KAFKA_CONTAINER kafka-consumer-groups \
        --bootstrap-server localhost:9092 \
        --describe \
        --group fraud-inference-group 2>/dev/null || print_warn "Consumer group not found"
}

# Function to view service logs
view_logs() {
    local lines=${1:-50}
    print_info "Last $lines lines of inference service logs:"
    docker-compose logs --tail=$lines inference-consumer
}

# Function to display service statistics
show_statistics() {
    print_info "Service Statistics:"
    echo "===================="
    
    # Container status
    docker-compose ps inference-consumer
    
    echo ""
    print_info "Resource Usage:"
    docker stats --no-stream inference-consumer 2>/dev/null || print_warn "Container not running"
    
    echo ""
    show_consumer_lag
}

# Main menu
show_menu() {
    echo ""
    echo "===================================="
    echo "Fraud Detection Inference Service"
    echo "Setup and Monitoring Tool"
    echo "===================================="
    echo "1. Check prerequisites"
    echo "2. Setup Kafka topics"
    echo "3. Check MLflow models"
    echo "4. Start inference service"
    echo "5. Send test transaction"
    echo "6. Monitor output (10s)"
    echo "7. View service logs"
    echo "8. Show statistics"
    echo "9. Full setup (1-4)"
    echo "10. Test end-to-end (5-6)"
    echo "0. Exit"
    echo "===================================="
}

# Main script logic
main() {
    if [ "$1" == "--auto" ]; then
        print_info "Running automatic setup..."
        check_service "kafka" && check_service "mlflow-server" || exit 1
        create_topics
        check_mlflow_model || print_warn "Consider training a model first"
        print_info "Starting inference service..."
        docker-compose up -d inference-consumer
        sleep 5
        wait_for_service "inference-consumer" 30
        print_info "Setup complete!"
        exit 0
    fi
    
    while true; do
        show_menu
        read -p "Enter your choice: " choice
        
        case $choice in
            1)
                print_info "Checking prerequisites..."
                check_service "kafka"
                check_service "mlflow-server"
                check_service "postgres"
                ;;
            2)
                create_topics
                ;;
            3)
                check_mlflow_model
                ;;
            4)
                print_info "Starting inference service..."
                docker-compose up -d inference-consumer
                wait_for_service "inference-consumer" 30
                ;;
            5)
                send_test_transaction
                ;;
            6)
                monitor_output 10
                ;;
            7)
                view_logs 100
                ;;
            8)
                show_statistics
                ;;
            9)
                print_info "Running full setup..."
                check_service "kafka" && check_service "mlflow-server" || continue
                create_topics
                check_mlflow_model
                docker-compose up -d inference-consumer
                wait_for_service "inference-consumer" 30
                print_info "Full setup complete!"
                ;;
            10)
                print_info "Running end-to-end test..."
                send_test_transaction
                sleep 2
                monitor_output 10
                ;;
            0)
                print_info "Exiting..."
                exit 0
                ;;
            *)
                print_error "Invalid choice. Please try again."
                ;;
        esac
        
        echo ""
        read -p "Press Enter to continue..."
    done
}

# Run main function
main "$@"