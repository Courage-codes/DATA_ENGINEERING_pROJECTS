#!/usr/bin/env python3
"""
Data Generator for Heart Rate Monitoring System
Generates synthetic heart rate data for multiple customers
"""
import time
import random
import datetime
import json
from faker import Faker
from kafka_producer import HeartRateProducer

fake = Faker()

class HeartRateGenerator:
    """Generate synthetic heart rate data for a set of customers"""
    
    def __init__(self, num_customers=100):
        """Initialize with a fixed number of customers"""
        self.num_customers = num_customers
        self.customers = list(range(1, num_customers + 1))
        
    def generate_heart_rate(self):
        """Generate a random heart rate, mostly within normal range"""
        # Normal distribution centered around 75 bpm
        # Occasionally generate outliers
        if random.random() < 0.05:  # 5% chance of outlier
            return random.choice([
                random.randint(30, 40),  # Low outlier
                random.randint(120, 200)  # High outlier
            ])
        else:
            return max(40, min(180, int(random.normalvariate(75, 10))))

    def generate_data_point(self):
        """Generate a single heart rate data point"""
        customer_id = random.choice(self.customers)
        heart_rate = self.generate_heart_rate()
        timestamp = datetime.datetime.now().isoformat()
        
        return {
            "customer_id": customer_id,
            "timestamp": timestamp,
            "heart_rate": heart_rate
        }

def main():
    """Main function to run the data generator"""
    generator = HeartRateGenerator()
    producer = HeartRateProducer()
    
    print("Starting heart rate data generation...")
    
    try:
        while True:
            data_point = generator.generate_data_point()
            print(f"Generated: {data_point}")
            producer.send_message(data_point)
            # Sleep for a random time between 0.1 and 0.5 seconds
            time.sleep(random.uniform(0.1, 0.5))
    except KeyboardInterrupt:
        print("Data generation stopped.")
    finally:
        producer.close()

if __name__ == "__main__":
    main()
