#!/usr/bin/env python3
"""
E-commerce Event Data Generator
Generates realistic e-commerce events and writes them to CSV files in /data/incoming.
"""

import os
import csv
import time
import random
import uuid
import argparse
from datetime import datetime
from typing import Dict, List, Any
from faker import Faker

# Event types and product categories used for generating realistic events
EVENT_TYPES = ["product_view", "add_to_cart", "purchase", "wishlist_add", "search"]
PRODUCT_CATEGORIES = [
    "Electronics", "Clothing", "Home & Kitchen", "Books", "Sports", "Beauty", 
    "Toys", "Jewelry", "Automotive", "Garden", "Office Supplies"
]

# Fixed CSV header fields matching the Spark schema exactly
FIELDNAMES = [
    "event_id",
    "event_type",
    "user_id",
    "product_id",
    "category",
    "price",
    "timestamp",
    "session_id",
    "ip_address",
    "user_agent",
    "search_query",
    "search_results_count",
    "quantity",
    "total_amount",
    "payment_method"
]

class EcommerceEventGenerator:
    def __init__(self, output_dir: str, interval: float = 1.0, batch_size: int = 10):
        """
        Initialize the event generator.
        
        Args:
            output_dir (str): Directory where CSV files will be written.
            interval (float): Time interval between batches in seconds.
            batch_size (int): Number of events per batch.
        """
        self.output_dir = output_dir
        self.interval = interval
        self.batch_size = batch_size
        self.faker = Faker()
        os.makedirs(output_dir, exist_ok=True)  # Ensure output directory exists
        self.users = self._generate_users(1000)  # Pre-generate user profiles
        self.products = self._generate_products(500)  # Pre-generate product catalog

    def _generate_users(self, count: int) -> List[Dict[str, Any]]:
        """
        Generate a list of fake user profiles.
        
        Args:
            count (int): Number of users to generate.
        
        Returns:
            List of user dictionaries.
        """
        return [{
            "user_id": str(uuid.uuid4()),
            "name": self.faker.name(),
            "email": self.faker.email(),
            "country": self.faker.country()
        } for _ in range(count)]

    def _generate_products(self, count: int) -> List[Dict[str, Any]]:
        """
        Generate a list of fake products.
        
        Args:
            count (int): Number of products to generate.
        
        Returns:
            List of product dictionaries.
        """
        return [{
            "product_id": str(uuid.uuid4()),
            "name": self.faker.bs(),
            "category": random.choice(PRODUCT_CATEGORIES),
            "price": round(random.uniform(5.99, 999.99), 2),
            "description": self.faker.text(max_nb_chars=100)
        } for _ in range(count)]

    def generate_event(self) -> Dict[str, Any]:
        """
        Generate a single e-commerce event with realistic data.
        
        Returns:
            Dictionary representing an event with all required fields.
        """
        user = random.choice(self.users)
        product = random.choice(self.products)
        event_type = random.choice(EVENT_TYPES)

        # Initialize event with all fields set to None to maintain CSV consistency
        event = dict.fromkeys(FIELDNAMES, None)

        # Populate common event fields
        event.update({
            "event_id": str(uuid.uuid4()),
            "event_type": event_type,
            "user_id": user["user_id"],
            "product_id": product["product_id"],
            "category": product["category"],
            "price": product["price"],
            "timestamp": datetime.now().isoformat(),
            "session_id": str(uuid.uuid4())[:8],
            "ip_address": self.faker.ipv4(),
            "user_agent": self.faker.user_agent()
        })

        # Add search-specific fields if event type is 'search'
        if event_type == "search":
            event["search_query"] = self.faker.word()
            event["search_results_count"] = random.randint(0, 200)

        # Add purchase-specific fields if event type is 'purchase'
        if event_type == "purchase":
            event["quantity"] = random.randint(1, 5)
            event["total_amount"] = round(event["price"] * event["quantity"], 2)
            event["payment_method"] = random.choice(["credit_card", "paypal", "apple_pay", "google_pay"])

        return event

    def generate_batch(self) -> List[Dict[str, Any]]:
        """
        Generate a batch of events.
        
        Returns:
            List of event dictionaries.
        """
        return [self.generate_event() for _ in range(self.batch_size)]

    def write_events_to_csv(self, events: List[Dict[str, Any]]):
        """
        Write a batch of events to a CSV file atomically.
        
        Args:
            events (List[Dict[str, Any]]): List of event dictionaries to write.
        """
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"events_{timestamp}.csv"
        temp_filepath = os.path.join(self.output_dir, f".{filename}.tmp")
        final_filepath = os.path.join(self.output_dir, filename)

        # Write to a temporary file first to avoid partial reads
        with open(temp_filepath, 'w', newline='') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=FIELDNAMES)
            writer.writeheader()
            writer.writerows(events)

        # Atomically rename temp file to final filename
        os.rename(temp_filepath, final_filepath)
        print(f"Generated {len(events)} events to {final_filepath}")

    def run(self, duration: int = None):
        """
        Run the generator continuously or for a specified duration.
        
        Args:
            duration (int, optional): Duration to run in seconds. Runs indefinitely if None.
        """
        start_time = time.time()
        count = 0
        try:
            while duration is None or time.time() - start_time < duration:
                events = self.generate_batch()
                self.write_events_to_csv(events)
                count += len(events)
                time.sleep(self.interval)
        except KeyboardInterrupt:
            print("\nGenerator stopped by user")
        print(f"Generated {count} events total")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate e-commerce event data")
    parser.add_argument("--output-dir", default="/data/incoming", help="Output directory for CSV files")
    parser.add_argument("--interval", type=float, default=5.0, help="Time interval between batches in seconds")
    parser.add_argument("--batch-size", type=int, default=10, help="Number of events to generate in each batch")
    parser.add_argument("--duration", type=int, default=None, help="Duration to run in seconds (default: indefinite)")
    args = parser.parse_args()

    generator = EcommerceEventGenerator(
        output_dir=args.output_dir,
        interval=args.interval,
        batch_size=args.batch_size
    )
    print(f"Starting generator with interval={args.interval}s, batch_size={args.batch_size}, output_dir={args.output_dir}")
    generator.run(duration=args.duration)

