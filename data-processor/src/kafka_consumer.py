#!/usr/bin/env python3
"""
Kafka Consumer for Heart Rate Monitoring System
Consumes heart rate data from Kafka topic and processes it
"""
import os
import json
import time
import logging
from kafka import KafkaConsumer
from kafka.errors import KafkaError
from db_connector import DatabaseConnector

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('heart_rate_consumer')

class HeartRateConsumer:
    """Consumes heart rate data from Kafka topic"""
    
    def __init__(self):
        """Initialize the Kafka consumer"""
        # Get Kafka broker address from environment variable or use default
        kafka_broker = os.environ.get('KAFKA_BROKER', 'kafka:9092')
        self.topic = os.environ.get('KAFKA_TOPIC', 'heartbeat-data')
        
        # Connect to database
        self.db = DatabaseConnector()
        
        # Keep trying to connect to Kafka until available
        connected = False
        retry_count = 0
        max_retries = 30
        
        while not connected and retry_count < max_retries:
            try:
                logger.info(f"Attempting to connect to Kafka broker at {kafka_broker}, attempt {retry_count+1}/{max_retries}")
                self.consumer = KafkaConsumer(
                    bootstrap_servers=[kafka_broker],
                    value_deserializer=lambda v: json.loads(v.decode('utf-8')),
                    auto_offset_reset='earliest',
                    group_id='heartbeat-processor',
                    enable_auto_commit=True,
                    auto_commit_interval_ms=5000
                )
                
                # Check if topic exists, create if not
                topics = self.consumer.topics()
                if self.topic not in topics:
                    logger.warning(f"Topic {self.topic} does not exist. Will be created when producer sends messages.")
                
                # Subscribe to the topic
                self.consumer.subscribe([self.topic])
                connected = True
                logger.info(f"Connected to Kafka broker at {kafka_broker}, subscribed to topic {self.topic}")
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

    def validate_heart_rate(self, heart_rate):
        """
        Validate heart rate is within acceptable limits
        Returns True if valid, False otherwise
        """
        # Heart rates below 40 or above 180 are considered outliers
        return 40 <= heart_rate <= 180
    
    def process_message(self, message):
        """Process a single message from Kafka"""
        try:
            # Extract data from message
            data = message.value
            
            # Validate heart rate
            if self.validate_heart_rate(data['heart_rate']):
                # Store valid data in database
                self.db.insert_heart_rate(
                    customer_id=data['customer_id'],
                    timestamp=data['timestamp'],
                    heart_rate=data['heart_rate']
                )
                logger.info(f"Processed and stored valid heart rate: {data}")
            else:
                # Log invalid heart rate
                logger.warning(f"Invalid heart rate detected: {data}")
                
            return True
        except Exception as e:
            logger.error(f"Error processing message: {e}")
            return False
    
    def start_consuming(self):
        """Start consuming messages from Kafka topic"""
        try:
            logger.info(f"Starting to consume messages from topic {self.topic}...")
            for message in self.consumer:
                self.process_message(message)
        except KeyboardInterrupt:
            logger.info("Consumption stopped by user")
        finally:
            if hasattr(self, 'consumer'):
                self.consumer.close()
                logger.info("Kafka consumer closed")
            self.db.close()

def main():
    """Main function to run the data consumer"""
    logger.info("Starting Heart Rate Consumer service")
    consumer = HeartRateConsumer()
    consumer.start_consuming()

if __name__ == "__main__":
    main()
