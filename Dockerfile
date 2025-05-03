# Multi-stage Dockerfile for ecommerce real-time pipeline

# ---------------------------
# Base Python image
# ---------------------------
FROM python:3.9-slim as python-base

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        build-essential \
    && rm -rf /var/lib/apt/lists/*

# ---------------------------
# Data Generator Stage
# ---------------------------
FROM python-base as data-generator

# Create target directory explicitly
RUN mkdir -p /generator

WORKDIR /generator

# Copy data generator script
COPY generator/data_generator.py /generator/data_generator.py

# Install data generator dependencies
RUN pip install --no-cache-dir faker

# ---------------------------
# File Mover Stage
# ---------------------------
FROM python-base as file-mover

# Create target directory explicitly
RUN mkdir -p /spark

WORKDIR /spark

# Copy file mover script
COPY spark/file_mover.py /spark/file_mover.py

# ---------------------------
# Spark Base Stage
# ---------------------------
FROM bitnami/spark:3.3.2 as spark-base

USER root

# Install Python dependencies for Spark job
RUN pip install --no-cache-dir pyspark==3.3.2 pandas numpy

# ---------------------------
# Spark Streaming Stage
# ---------------------------
FROM spark-base as spark-streaming

WORKDIR /app

# Copy Spark application files
COPY spark/spark_streaming_to_postgres.py /app/
COPY spark/log4j.properties /opt/bitnami/spark/conf/log4j.properties

# Download PostgreSQL JDBC driver
RUN apt-get update && apt-get install -y wget && \
    wget -L -O /opt/bitnami/spark/jars/postgresql-42.5.1.jar https://jdbc.postgresql.org/download/postgresql-42.5.1.jar && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# Create directories for data and checkpoints with proper permissions
RUN mkdir -p /data /checkpoints && chmod -R 777 /data /checkpoints

# Set environment variables for Spark job
ENV POSTGRES_HOST=postgres \
    POSTGRES_PORT=5432 \
    POSTGRES_DB=ecommerce \
    POSTGRES_USER=postgres \
    POSTGRES_PASSWORD=postgres \
    INPUT_PATH=/data/processing \
    CHECKPOINT_PATH=/checkpoints

# Run Spark as non-root user
USER 1001

