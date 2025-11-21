"""
Integration test script for the Fraud Detection Inference Service.

This script tests the end-to-end flow by:
1. Producing test transactions to Kafka
2. Waiting for the inference service to process them
3. Consuming the scored results
4. Validating the output format and values
"""

import json
import time
import argparse
from datetime import datetime
from typing import List, Dict, Any

from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import KafkaError


class InferenceServiceTester:
    """Test harness for the inference service."""
    
    def __init__(self, bootstrap_servers: str = 'localhost:9092'):
        """Initialize the tester with Kafka connection."""
        self.bootstrap_servers = bootstrap_servers
        self.input_topic = 'raw_transactions'
        self.output_topic = 'scored_transactions'
        self.producer = None
        self.consumer = None
        
    def connect(self):
        """Connect to Kafka."""
        print(f"Connecting to Kafka at {self.bootstrap_servers}...")
        
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=self.bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
            print("✓ Producer connected")
            
            self.consumer = KafkaConsumer(
                self.output_topic,
                bootstrap_servers=self.bootstrap_servers,
                auto_offset_reset='latest',
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                consumer_timeout_ms=30000  # 30 second timeout
            )
            print("✓ Consumer connected")
            
        except Exception as e:
            print(f"✗ Failed to connect to Kafka: {e}")
            raise
    
    def generate_test_transactions(self, count: int = 5) -> List[Dict[str, Any]]:
        """Generate sample test transactions."""
        transactions = []
        
        # Normal transaction
        transactions.append({
            'transaction_id': f'test_normal_{int(time.time())}',
            'amount': 50.00,
            'merchant_category': 'grocery',
            'transaction_hour': 10,
            'day_of_week': 2,
            'distance_from_home': 2.0,
            'distance_from_last_transaction': 1.5,
            'ratio_to_median_purchase_price': 0.8,
            'repeat_retailer': 1,
            'used_chip': 1,
            'used_pin_number': 1,
            'online_order': 0
        })
        
        # Potentially fraudulent transaction (large amount, far from home)
        transactions.append({
            'transaction_id': f'test_suspicious_{int(time.time())}',
            'amount': 5000.00,
            'merchant_category': 'electronics',
            'transaction_hour': 3,  # Late night
            'day_of_week': 6,
            'distance_from_home': 500.0,  # Very far
            'distance_from_last_transaction': 450.0,
            'ratio_to_median_purchase_price': 10.0,  # Much higher than usual
            'repeat_retailer': 0,
            'used_chip': 0,
            'used_pin_number': 0,
            'online_order': 1
        })
        
        # Add more varied transactions
        for i in range(count - 2):
            transactions.append({
                'transaction_id': f'test_varied_{i}_{int(time.time())}',
                'amount': 100.0 + (i * 50),
                'merchant_category': ['retail', 'gas', 'restaurant'][i % 3],
                'transaction_hour': 8 + (i * 2),
                'day_of_week': i % 7,
                'distance_from_home': 5.0 + i,
                'distance_from_last_transaction': 2.0 + i,
                'ratio_to_median_purchase_price': 1.0 + (i * 0.2),
                'repeat_retailer': i % 2,
                'used_chip': 1,
                'used_pin_number': 1,
                'online_order': i % 2
            })
        
        return transactions
    
    def send_transactions(self, transactions: List[Dict[str, Any]]) -> bool:
        """Send test transactions to Kafka."""
        print(f"\nSending {len(transactions)} test transactions...")
        
        try:
            for i, transaction in enumerate(transactions):
                self.producer.send(self.input_topic, value=transaction)
                print(f"  [{i+1}/{len(transactions)}] Sent: {transaction['transaction_id']}")
            
            self.producer.flush()
            print("✓ All transactions sent successfully")
            return True
            
        except Exception as e:
            print(f"✗ Failed to send transactions: {e}")
            return False
    
    def consume_results(self, expected_count: int, timeout: int = 30) -> List[Dict[str, Any]]:
        """Consume scored results from Kafka."""
        print(f"\nWaiting for {expected_count} scored results (timeout: {timeout}s)...")
        
        results = []
        start_time = time.time()
        
        try:
            for message in self.consumer:
                results.append(message.value)
                elapsed = time.time() - start_time
                print(f"  [{len(results)}/{expected_count}] Received result "
                      f"(elapsed: {elapsed:.1f}s)")
                
                if len(results) >= expected_count:
                    break
                    
                if elapsed > timeout:
                    print(f"✗ Timeout reached after {timeout}s")
                    break
            
            if len(results) == expected_count:
                print(f"✓ Received all {expected_count} results")
            else:
                print(f"⚠ Received {len(results)}/{expected_count} results")
            
            return results
            
        except Exception as e:
            print(f"✗ Error consuming results: {e}")
            return results
    
    def validate_result(self, result: Dict[str, Any]) -> bool:
        """Validate a single scored result."""
        required_fields = [
            'transaction_id',
            'original_data',
            'prediction',
            'fraud_probability',
            'is_fraud',
            'model_version',
            'scored_at'
        ]
        
        # Check all required fields are present
        missing_fields = [f for f in required_fields if f not in result]
        if missing_fields:
            print(f"  ✗ Missing fields: {missing_fields}")
            return False
        
        # Validate field types and values
        try:
            assert isinstance(result['prediction'], int), "prediction must be int"
            assert result['prediction'] in [0, 1], "prediction must be 0 or 1"
            
            assert isinstance(result['fraud_probability'], (int, float)), \
                "fraud_probability must be numeric"
            assert 0 <= result['fraud_probability'] <= 1, \
                "fraud_probability must be between 0 and 1"
            
            assert isinstance(result['is_fraud'], bool), "is_fraud must be boolean"
            assert result['is_fraud'] == (result['prediction'] == 1), \
                "is_fraud must match prediction"
            
            assert isinstance(result['model_version'], (int, str)), \
                "model_version must be int or str"
            
            # Validate timestamp format
            datetime.fromisoformat(result['scored_at'].replace('Z', '+00:00'))
            
            return True
            
        except AssertionError as e:
            print(f"  ✗ Validation error: {e}")
            return False
        except Exception as e:
            print(f"  ✗ Unexpected error: {e}")
            return False
    
    def validate_results(self, results: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Validate all scored results and return statistics."""
        print("\n" + "="*60)
        print("VALIDATION RESULTS")
        print("="*60)
        
        stats = {
            'total': len(results),
            'valid': 0,
            'invalid': 0,
            'fraud_detected': 0,
            'legitimate': 0,
            'avg_fraud_probability': 0.0,
            'model_versions': set()
        }
        
        fraud_probs = []
        
        for i, result in enumerate(results):
            print(f"\nResult {i+1}: {result.get('transaction_id', 'unknown')}")
            
            if self.validate_result(result):
                stats['valid'] += 1
                print("  ✓ Valid")
            else:
                stats['invalid'] += 1
                print("  ✗ Invalid")
                continue
            
            # Collect statistics
            if result['is_fraud']:
                stats['fraud_detected'] += 1
                print(f"  ⚠ FRAUD DETECTED (probability: {result['fraud_probability']:.3f})")
            else:
                stats['legitimate'] += 1
                print(f"  ✓ Legitimate (probability: {result['fraud_probability']:.3f})")
            
            fraud_probs.append(result['fraud_probability'])
            stats['model_versions'].add(str(result['model_version']))
        
        # Calculate averages
        if fraud_probs:
            stats['avg_fraud_probability'] = sum(fraud_probs) / len(fraud_probs)
        
        return stats
    
    def print_summary(self, stats: Dict[str, Any]):
        """Print test summary."""
        print("\n" + "="*60)
        print("TEST SUMMARY")
        print("="*60)
        print(f"Total results:           {stats['total']}")
        print(f"Valid results:           {stats['valid']}")
        print(f"Invalid results:         {stats['invalid']}")
        print(f"Fraud detected:          {stats['fraud_detected']}")
        print(f"Legitimate:              {stats['legitimate']}")
        print(f"Avg fraud probability:   {stats['avg_fraud_probability']:.3f}")
        print(f"Model versions used:     {', '.join(stats['model_versions'])}")
        
        # Overall result
        print("\n" + "="*60)
        if stats['invalid'] == 0 and stats['valid'] == stats['total']:
            print("✓ ALL TESTS PASSED")
        else:
            print("⚠ SOME TESTS FAILED")
        print("="*60 + "\n")
    
    def run_test(self, transaction_count: int = 5) -> bool:
        """Run the complete test suite."""
        print("="*60)
        print("Fraud Detection Inference Service - Integration Test")
        print("="*60)
        
        try:
            # Connect
            self.connect()
            
            # Generate and send transactions
            transactions = self.generate_test_transactions(transaction_count)
            if not self.send_transactions(transactions):
                return False
            
            # Wait a bit for processing
            print("\nWaiting 5 seconds for processing...")
            time.sleep(5)
            
            # Consume results
            results = self.consume_results(len(transactions), timeout=30)
            
            if not results:
                print("✗ No results received")
                return False
            
            # Validate results
            stats = self.validate_results(results)
            
            # Print summary
            self.print_summary(stats)
            
            # Return success if all results are valid
            return stats['invalid'] == 0 and stats['valid'] > 0
            
        except Exception as e:
            print(f"\n✗ Test failed with error: {e}")
            import traceback
            traceback.print_exc()
            return False
        
        finally:
            if self.producer:
                self.producer.close()
            if self.consumer:
                self.consumer.close()


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description='Test the Fraud Detection Inference Service'
    )
    parser.add_argument(
        '--bootstrap-servers',
        default='localhost:9092',
        help='Kafka bootstrap servers (default: localhost:9092)'
    )
    parser.add_argument(
        '--count',
        type=int,
        default=5,
        help='Number of test transactions to send (default: 5)'
    )
    
    args = parser.parse_args()
    
    tester = InferenceServiceTester(bootstrap_servers=args.bootstrap_servers)
    success = tester.run_test(transaction_count=args.count)
    
    # Exit with appropriate code
    exit(0 if success else 1)


if __name__ == '__main__':
    main()