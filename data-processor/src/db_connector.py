#!/usr/bin/env python3
"""
Database Connector for Heart Rate Monitoring System
Handles connections and operations with PostgreSQL database
"""
import os
import time
import logging
import psycopg2
from psycopg2 import sql
from datetime import datetime

# Ensure the log directory exists
log_dir = '/app/logs'
os.makedirs(log_dir, exist_ok=True)

# Configure logging to file
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    filename=os.path.join(log_dir, 'database_connector.log'),
    filemode='a'  # Append mode
)

logger = logging.getLogger('database_connector')

class DatabaseConnector:
    """Connects to and interacts with PostgreSQL database"""
    
    def __init__(self):
        """Initialize the database connection"""
        self.db_params = {
            'host': os.environ.get('DB_HOST'),
            'port': os.environ.get('DB_PORT'),
            'database': os.environ.get('DB_NAME'),
            'user': os.environ.get('DB_USER'),
            'password': os.environ.get('DB_PASSWORD')
        }
        self.conn = None
        self.connect_with_retry()
    
    def connect_with_retry(self, max_retries=10, retry_delay=5):
        """Attempt to connect to the database with retries"""
        retries = 0
        while retries < max_retries:
            try:
                self.conn = psycopg2.connect(**self.db_params)
                self.conn.autocommit = True
                logger.info("Successfully connected to PostgreSQL database")
                return True
            except psycopg2.OperationalError as e:
                retries += 1
                logger.warning(f"Database connection failed (attempt {retries}/{max_retries}): {e}")
                if retries < max_retries:
                    logger.info(f"Retrying in {retry_delay} seconds...")
                    time.sleep(retry_delay)
                else:
                    logger.error("Maximum retry attempts reached. Could not connect to database.")
                    raise
    
    def insert_heart_rate(self, customer_id, timestamp, heart_rate):
        """Insert or update a heart rate record into the database (UPSERT)"""
        try:
            if not self.is_connected():
                logger.info("Database connection lost. Reconnecting...")
                self.connect_with_retry()
                
            if isinstance(timestamp, str):
                timestamp = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
                
            with self.conn.cursor() as cursor:
                upsert_sql = sql.SQL("""
                    INSERT INTO heartbeats (customer_id, timestamp, heart_rate)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (customer_id, timestamp) DO UPDATE
                    SET heart_rate = EXCLUDED.heart_rate
                    RETURNING id
                """)
                cursor.execute(upsert_sql, (customer_id, timestamp, heart_rate))
                record_id = cursor.fetchone()[0]
                logger.info(f"Upserted heart rate record ID {record_id} for customer {customer_id}")
                return record_id
        except Exception as e:
            logger.error(f"Error upserting heart rate record: {e}", exc_info=True)
            # Optional: retry once after reconnect
            try:
                self.connect_with_retry()
                with self.conn.cursor() as cursor:
                    cursor.execute(upsert_sql, (customer_id, timestamp, heart_rate))
                    record_id = cursor.fetchone()[0]
                    logger.info(f"Upserted heart rate record ID {record_id} after reconnect")
                    return record_id
            except Exception as e2:
                logger.error(f"Retry failed: {e2}", exc_info=True)
                return None
    
    def is_connected(self):
        """Check if database connection is still active"""
        if not self.conn:
            return False
        try:
            with self.conn.cursor() as cursor:
                cursor.execute("SELECT 1")
                return True
        except psycopg2.Error:
            return False
    
    def close(self):
        """Close the database connection"""
        if self.conn:
            self.conn.close()
            logger.info("Database connection closed")

if __name__ == "__main__":
    db = DatabaseConnector()
    record_id = db.insert_heart_rate(
        customer_id=1,
        timestamp=datetime.now(),
        heart_rate=75
    )
    logger.info(f"Inserted record with ID: {record_id}")
    db.close()
