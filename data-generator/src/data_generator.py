#!/usr/bin/env python3
"""
Data Generator for Heart Rate Monitoring System
Generates synthetic heart rate data for multiple customers
"""
import time
import random
import datetime
import os
import logging
from faker import Faker
from kafka_producer import HeartRateProducer

fake = Faker()

# Setup logging
log_dir = '/app/logs'
os.makedirs(log_dir, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(log_dir, 'data_generator.log')),
        logging.StreamHandler()  # Also output logs to console
    ]
)
logger = logging.getLogger('data_generator')

class HeartRateGenerator:
    """Generate synthetic heart rate data for a set of customers"""
    
    def __init__(self, num_customers=100):
        self.num_customers = num_customers
        self.customers = list(range(1, num_customers + 1))
        
    def generate_heart_rate(self):
        if random.random() < 0.05:  # 5% chance of outlier
            return random.choice([
                random.randint(30, 40),  # Low outlier
                random.randint(120, 200)  # High outlier
            ])
        else:
            return max(40, min(180, int(random.normalvariate(75, 10))))

    def generate_data_point(self):
        customer_id = random.choice(self.customers)
        heart_rate = self.generate_heart_rate()
        timestamp = datetime.datetime.now().isoformat()
        
        return {
            "customer_id": customer_id,
            "timestamp": timestamp,
            "heart_rate": heart_rate
        }

def main():
    generator = HeartRateGenerator()
    producer = HeartRateProducer()
    
    logger.info("Starting heart rate data generation...")
    count = 0
    
    try:
        while True:
            data_point = generator.generate_data_point()
            logger.debug(f"Generated data point #{count}: {data_point}")
            try:
                success = producer.send_message(data_point)
                if success:
                    logger.info(f"Sent data point #{count} successfully")
                else:
                    logger.warning(f"Failed to send data point #{count}")
            except Exception as e:
                logger.error(f"Exception sending data point #{count}: {e}", exc_info=True)
            count += 1
            
            # Adjust sleep time here for faster/slower generation
            time.sleep(random.uniform(0.01, 0.05))  # 20-100 messages per second approx.
            
    except KeyboardInterrupt:
        logger.info("Data generation stopped by user")
    except Exception as e:
        logger.error(f"Unexpected error in data generation loop: {e}", exc_info=True)
    finally:
        producer.close()
        logger.info(f"Total data points generated: {count}")

if __name__ == "__main__":
    main()
