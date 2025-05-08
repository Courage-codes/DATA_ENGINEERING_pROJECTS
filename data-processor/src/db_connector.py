#!/usr/bin/env python3
"""
Database Connector for Heart Rate Monitoring System
Handles connections and operations with PostgreSQL database
"""
import os
import time
import psycopg2
from psycopg2 import sql
from datetime import datetime

class DatabaseConnector:
    """Connects to and interacts with PostgreSQL database"""
    
    def __init__(self):
        """Initialize the database connection"""
        # Database connection parameters from environment variables
        self.db_params = {
            'host': os.environ.get('DB_HOST'),
            'port': os.environ.get('DB_PORT'),
            'database': os.environ.get('DB_NAME'),
            'user': os.environ.get('DB_USER'),
            'password': os.environ.get('DB_PASSWORD')
        }
        
        # Connect to database with retry mechanism
        self.connect_with_retry()
    
    def connect_with_retry(self, max_retries=10, retry_delay=5):
        """Attempt to connect to the database with retries"""
        retries = 0
        
        while retries < max_retries:
            try:
                self.conn = psycopg2.connect(**self.db_params)
                self.conn.autocommit = True
                print("Successfully connected to PostgreSQL database")
                return True
            except psycopg2.OperationalError as e:
                retries += 1
                print(f"Database connection failed (attempt {retries}/{max_retries}): {e}")
                if retries < max_retries:
                    print(f"Retrying in {retry_delay} seconds...")
                    time.sleep(retry_delay)
                else:
                    print("Maximum retry attempts reached. Could not connect to database.")
                    raise
    
    def insert_heart_rate(self, customer_id, timestamp, heart_rate):
        """Insert a heart rate record into the database"""
        try:
            # Ensure connection is active
            if not self.is_connected():
                self.connect_with_retry()
                
            # Parse timestamp if it's a string
            if isinstance(timestamp, str):
                timestamp = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
                
            # Create a cursor
            with self.conn.cursor() as cursor:
                # SQL for inserting a heart rate record
                insert_sql = sql.SQL("""
                    INSERT INTO heartbeats (customer_id, timestamp, heart_rate)
                    VALUES (%s, %s, %s)
                    RETURNING id
                """)
                
                # Execute the insert
                cursor.execute(insert_sql, (customer_id, timestamp, heart_rate))
                
                # Get the inserted record ID
                record_id = cursor.fetchone()[0]
                
                return record_id
        except Exception as e:
            print(f"Error inserting heart rate record: {e}")
            # Attempt to reconnect on failure
            self.connect_with_retry()
            return None
    
    def is_connected(self):
        """Check if database connection is still active"""
        if not hasattr(self, 'conn'):
            return False
            
        try:
            # Try a simple query to check connection
            with self.conn.cursor() as cursor:
                cursor.execute("SELECT 1")
                return True
        except psycopg2.Error:
            return False
    
    def close(self):
        """Close the database connection"""
        if hasattr(self, 'conn') and self.conn:
            self.conn.close()
            print("Database connection closed")

if __name__ == "__main__":
    # Test the database connector
    db = DatabaseConnector()
    record_id = db.insert_heart_rate(
        customer_id=1,
        timestamp=datetime.now(),
        heart_rate=75
    )
    print(f"Inserted record with ID: {record_id}")
    db.close()
