#!/usr/bin/env python3
"""
Kafka Producer for Heart Rate Monitoring System
Sends heart rate data to Kafka topic
"""
import os
import json
import time
import logging
from kafka import KafkaProducer
from kafka.errors import KafkaError

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('heart_rate_producer')

class HeartRateProducer:
    """Produces heart rate data to Kafka topic"""
    
    def __init__(self):
        """Initialize the Kafka producer"""
        # Get Kafka broker address from environment variable or use default
        kafka_broker = os.environ.get('KAFKA_BROKER', 'kafka:9092')
        
        # Initialize the producer with retry capabilities
        self.topic = os.environ.get('KAFKA_TOPIC', 'heartbeat-data')
        
        # Keep trying to connect to Kafka until available
        connected = False
        retry_count = 0
        max_retries = 30
        
        while not connected and retry_count < max_retries:
            try:
                logger.info(f"Attempting to connect to Kafka broker at {kafka_broker}, attempt {retry_count+1}/{max_retries}")
                self.producer = KafkaProducer(
                    bootstrap_servers=[kafka_broker],
                    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                    acks='all',  # Wait for all replicas
                    retries=5,
                    retry_backoff_ms=500
                )
                connected = True
                logger.info(f"Connected to Kafka broker at {kafka_broker}")
            except KafkaError as e:
                retry_count += 1
                logger.warning(f"Failed to connect to Kafka: {e}. Retrying in 5 seconds... ({retry_count}/{max_retries})")
                time.sleep(5)
            except Exception as e:
                retry_count += 1
                logger.error(f"Unexpected error connecting to Kafka: {e}. Retrying in 5 seconds... ({retry_count}/{max_retries})")
                time.sleep(5)
        
        if not connected:
            logger.error("Failed to connect to Kafka after maximum retries. Exiting.")
            raise Exception("Failed to connect to Kafka")
    
    def send_message(self, data):
        """Send heart rate data to Kafka topic"""
        try:
            # Use customer_id as the key for partitioning
            key = str(data['customer_id']).encode('utf-8')
            
            # Send message asynchronously
            future = self.producer.send(
                self.topic, 
                key=key,
                value=data
            )
            
            # Block until the message is sent (or timeout)
            record_metadata = future.get(timeout=10)
            logger.debug(f"Message sent to topic: {record_metadata.topic}, partition: {record_metadata.partition}, offset: {record_metadata.offset}")
            return True
        except Exception as e:
            logger.error(f"Error sending message to Kafka: {e}")
            return False
    
    def close(self):
        """Close the Kafka producer"""
        if hasattr(self, 'producer'):
            self.producer.close()
            logger.info("Kafka producer closed")

if __name__ == "__main__":
    # Test the producer
    producer = HeartRateProducer()
    test_data = {
        "customer_id": 1,
        "timestamp": "2025-05-07T12:00:00",
        "heart_rate": 75
    }
    producer.send_message(test_data)
    producer.close()
