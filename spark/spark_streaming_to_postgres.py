#!/usr/bin/env python3
"""
Spark Structured Streaming to PostgreSQL Pipeline with robust file handling.
All configuration is loaded from environment variables; no hardcoded defaults.
"""

import os
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, current_timestamp, lit, when
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

# Load required environment variables; raise KeyError if any is missing
POSTGRES_HOST = os.environ["POSTGRES_HOST"]
POSTGRES_PORT = os.environ["POSTGRES_PORT"]
POSTGRES_DB = os.environ["POSTGRES_DB"]
POSTGRES_USER = os.environ["POSTGRES_USER"]
POSTGRES_PASSWORD = os.environ["POSTGRES_PASSWORD"]
PROCESSING_PATH = os.environ["PROCESSING_PATH"]
CHECKPOINT_PATH = os.environ["CHECKPOINT_PATH"]

POSTGRES_URL = f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
POSTGRES_DRIVER = "org.postgresql.Driver"

def create_spark_session():
    """
    Create and configure SparkSession for streaming.
    
    Returns:
        SparkSession object.
    """
    return (SparkSession.builder
            .appName("EcommerceStreamingToPostgres")
            .config("spark.jars.packages", "org.postgresql:postgresql:42.5.1")
            .config("spark.sql.streaming.checkpointLocation", CHECKPOINT_PATH)
            .config("spark.streaming.stopGracefullyOnShutdown", "true")
            .getOrCreate())

def define_event_schema():
    """
    Define the schema for incoming CSV event data.
    
    Returns:
        StructType schema matching CSV columns.
    """
    return StructType([
        StructField("event_id", StringType(), True),
        StructField("event_type", StringType(), True),
        StructField("user_id", StringType(), True),
        StructField("product_id", StringType(), True),
        StructField("category", StringType(), True),
        StructField("price", DoubleType(), True),
        StructField("timestamp", StringType(), True),
        StructField("session_id", StringType(), True),
        StructField("ip_address", StringType(), True),
        StructField("user_agent", StringType(), True),
        StructField("search_query", StringType(), True),
        StructField("search_results_count", IntegerType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("total_amount", DoubleType(), True),
        StructField("payment_method", StringType(), True)
    ])

def clean_transform_data(df):
    """
    Perform data cleaning and transformation on streaming DataFrame.
    
    Args:
        df: Raw streaming DataFrame.
    
    Returns:
        Transformed DataFrame ready for writing.
    """
    # Convert timestamp string to TimestampType (nullable safe)
    df = df.withColumn("processed_timestamp", to_timestamp(col("timestamp")))
    
    # Add processing time column with current timestamp
    df = df.withColumn("processing_time", current_timestamp())
    
    # For purchase events, ensure quantity is set, defaulting to 1 if null
    df = df.withColumn(
        "quantity", 
        when(col("event_type") == "purchase", 
             when(col("quantity").isNull(), lit(1)).otherwise(col("quantity")))
        .otherwise(lit(None))
    )
    
    # For purchase events, calculate total_amount if missing (price * quantity)
    df = df.withColumn(
        "total_amount", 
        when(col("event_type") == "purchase", 
             when(col("total_amount").isNull(), col("price") * col("quantity")).otherwise(col("total_amount")))
        .otherwise(lit(None))
    )
    
    # Filter out rows missing essential fields to avoid corrupt data
    df = df.filter(
        col("event_id").isNotNull() & 
        col("event_type").isNotNull() & 
        col("user_id").isNotNull() & 
        col("processed_timestamp").isNotNull()
    )
    
    # Drop original timestamp column as processed_timestamp is used
    df = df.drop("timestamp")
    
    return df

def log_files_in_processing_dir(epoch_id):
    """
    Log the files currently in the processing directory for debugging.
    
    Args:
        epoch_id: Batch identifier.
    """
    try:
        files = os.listdir(PROCESSING_PATH)
        print(f"Batch {epoch_id}: Files in processing directory: {files}")
    except Exception as e:
        print(f"Batch {epoch_id}: Could not list files in processing directory: {str(e)}")

def write_to_postgres(df, epoch_id, max_retries=3):
    """
    Write a batch of processed data to PostgreSQL with retry logic.
    
    Args:
        df: DataFrame to write.
        epoch_id: Batch identifier.
        max_retries: Number of retry attempts on failure.
    """
    attempt = 0
    while attempt < max_retries:
        try:
            log_files_in_processing_dir(epoch_id)
            count = df.count()
            if count > 0:
                df.write \
                    .format("jdbc") \
                    .option("url", POSTGRES_URL) \
                    .option("dbtable", "events") \
                    .option("user", POSTGRES_USER) \
                    .option("password", POSTGRES_PASSWORD) \
                    .option("driver", POSTGRES_DRIVER) \
                    .mode("append") \
                    .save()
                print(f"Batch {epoch_id}: Successfully wrote {count} records to PostgreSQL")
            else:
                print(f"Batch {epoch_id}: No records to write")
            break
        except Exception as e:
            attempt += 1
            print(f"Batch {epoch_id}: Error writing to PostgreSQL (attempt {attempt}): {e}")
            if attempt >= max_retries:
                print(f"Batch {epoch_id}: Max retries reached. Skipping batch.")
            else:
                time.sleep(5)

def main():
    """
    Main entry point for the Spark streaming job.
    """
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    # Ensure checkpoint and processing directories exist
    os.makedirs(CHECKPOINT_PATH, exist_ok=True)
    os.makedirs(PROCESSING_PATH, exist_ok=True)

    schema = define_event_schema()
    input_path_uri = f"file://{PROCESSING_PATH}"

    # Read streaming CSV files with explicit schema and permissive mode to handle corrupt/malformed files gracefully
    raw_stream_df = spark.readStream \
        .format("csv") \
        .option("header", "true") \
        .option("inferSchema", "false") \
        .option("mode", "PERMISSIVE") \
        .schema(schema) \
        .option("maxFilesPerTrigger", 1) \
        .load(input_path_uri)

    # Transform data before writing
    processed_df = clean_transform_data(raw_stream_df)

    # Write streaming data to PostgreSQL using foreachBatch for custom batch logic
    query = processed_df.writeStream \
        .foreachBatch(write_to_postgres) \
        .outputMode("append") \
        .option("checkpointLocation", CHECKPOINT_PATH) \
        .start()

    # Await termination of streaming query
    query.awaitTermination()

if __name__ == "__main__":
    # Wait a bit for PostgreSQL to be ready before starting streaming job
    time.sleep(10)
    main()

