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

# Ensure the log directory exists
log_dir = '/app/logs'
os.makedirs(log_dir, exist_ok=True)

# Configure logging to file
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    filename=os.path.join(log_dir, 'heart_rate_consumer.log'),
    filemode='a'  # Append mode
)
logger = logging.getLogger('heart_rate_consumer')

class HeartRateConsumer:
    """Consumes heart rate data from Kafka topic"""
    
    def __init__(self):
        """Initialize the Kafka consumer"""
        kafka_broker = os.environ.get('KAFKA_BROKER', 'kafka:9092')
        self.topic = os.environ.get('KAFKA_TOPIC', 'heartbeat-data')
        
        # Connect to database
        self.db = DatabaseConnector()
        
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
                
                topics = self.consumer.topics()
                if self.topic not in topics:
                    logger.warning(f"Topic {self.topic} does not exist. Will be created when producer sends messages.")
                
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
        return 40 <= heart_rate <= 180
    
    def process_message(self, message):
        """Process a single message from Kafka"""
        try:
            data = message.value
            
            if self.validate_heart_rate(data['heart_rate']):
                self.db.insert_heart_rate(
                    customer_id=data['customer_id'],
                    timestamp=data['timestamp'],
                    heart_rate=data['heart_rate']
                )
                logger.info(f"Processed and stored valid heart rate: {data}")
            else:
                logger.warning(f"Invalid heart rate detected: {data}")
                
            return True
        except Exception as e:
            logger.error(f"Error processing message: {e}", exc_info=True)
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
