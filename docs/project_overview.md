# Real-Time E-commerce Data Pipeline Project Overview

## Project Purpose
This project implements a real-time data processing pipeline for e-commerce event data. It captures, processes, and analyzes user interactions on an e-commerce platform (such as product views, purchases, searches) to enable real-time analytics and business intelligence.

## System Components

### 1. Data Generator
- Simulates real e-commerce user activity
- Produces realistic events with timestamps, user IDs, product details, etc.
- Writes events as CSV files to the `/data/incoming` directory
- Configurable batch size and generation interval

### 2. File Mover
- Monitors the `/data/incoming` directory for new CSV files
- Moves files to `/data/processing` directory to prevent partial reads 
- Provides a clean handoff between generation and processing stages

### 3. Spark Streaming Service
- Processes CSV files using Spark Structured Streaming
- Implements data validation, cleaning, and transformation
- Handles timestamp conversion and null value management
- Includes robust error handling and retry logic
- Maintains processing state via checkpointing

### 4. PostgreSQL Database
- Stores all processed e-commerce events in the `events` table
- Enables SQL querying for analytics and reporting
- Provides persistent storage for the processed data

## Data Flow

1. **Generation**: The data generator creates batches of synthetic e-commerce events and writes them to CSV files
2. **Transfer**: The file mover service transfers completed files to the processing directory
3. **Processing**: Spark Structured Streaming reads, validates, and transforms the data
4. **Storage**: Processed data is written to PostgreSQL tables for analysis and reporting

## Technology Stack

- **Data Generation**: Python with Faker library
- **Stream Processing**: Apache Spark (PySpark) with Structured Streaming
- **Database**: PostgreSQL 14
- **Infrastructure**: Docker with docker-compose for container orchestration
- **Language**: Python 3.9

## Key Features

- **Containerized Architecture**: All components run in Docker containers for portability and isolation
- **Fault Tolerance**: Checkpointing, idempotent processing, and retry logic ensure reliable data flow
- **Data Quality**: Validation and transformation ensure clean, consistent data
- **Scalability**: Components can be scaled independently as needed
- **Configurability**: Environment variables control behavior without code changes
