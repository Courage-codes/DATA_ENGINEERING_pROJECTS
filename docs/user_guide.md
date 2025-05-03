# Real-Time E-commerce Pipeline User Guide

This guide provides step-by-step instructions for setting up, running, and monitoring the real-time e-commerce data pipeline.

## Prerequisites

- Docker and Docker Compose installed on your system
- Git (optional, for cloning the repository)
- 4GB+ RAM available for Docker

## Getting Started

### Step 1: Clone or Download the Project

```bash
git clone <repository-url>
cd real-time-ecommerce-pipeline
```

### Step 2: Set Up Environment Variables

The project includes a default `.env` file with development settings. For custom configuration:

1. Make a copy of the `.env` file:
   ```bash
   cp .env .env.custom
   ```

2. Edit the settings as needed:
   ```bash
   nano .env.custom
   ```

3. Use your custom environment file:
   ```bash
   export ENV_FILE=.env.custom
   ```

### Step 3: Build and Start the Pipeline

Launch all services using Docker Compose:

```bash
docker-compose up -d
```

This command starts all four services in detached mode:
- PostgreSQL database (`ecommerce-postgres`)
- Data generator (`data-generator`)
- File mover (`file-mover`)
- Spark streaming processor (`spark-streaming`)

### Step 4: Monitor the Pipeline

#### View Logs

To see logs from all services:
```bash
docker-compose logs -f
```

For logs from a specific service:
```bash
docker-compose logs -f data-generator
docker-compose logs -f spark-streaming
```

#### Check Data Generation

Monitor the data generator's output:
```bash
docker-compose logs -f data-generator
```

#### Verify Data in PostgreSQL

Connect to the PostgreSQL database:
```bash
docker exec -it ecommerce-postgres psql -U postgres -d ecommerce
```

Query the events table:
```sql
SELECT COUNT(*) FROM events;
SELECT event_type, COUNT(*) FROM events GROUP BY event_type;
SELECT * FROM events LIMIT 10;
```

Exit PostgreSQL:
```
\q
```

### Step 5: Adjust Configuration (Optional)

To modify the data generator settings (batch size, interval):

1. Stop the services:
   ```bash
   docker-compose down
   ```

2. Edit `docker-compose.yml` to change the command arguments:
   ```yaml
   # Example: Change to 20 events every 10 seconds
   command: ["python", "/generator/data_generator.py", "--output-dir", "/data/incoming", "--interval", "10", "--batch-size", "20"]
   ```

3. Restart the services:
   ```bash
   docker-compose up -d
   ```

### Step 6: Stopping the Pipeline

To stop all services:
```bash
docker-compose down
```

To stop and remove all data volumes (CAUTION: this will delete all stored data):
```bash
docker-compose down -v
```

## Troubleshooting

### No Data in PostgreSQL

1. Check if the data generator is producing files:
   ```bash
   docker-compose logs data-generator
   ```

2. Verify the file mover is correctly transferring files:
   ```bash
   docker-compose logs file-mover
   ```

3. Check for errors in the Spark streaming logs:
   ```bash
   docker-compose logs spark-streaming
   ```

### Connection Issues

If you cannot connect to PostgreSQL:

1. Verify the container is running:
   ```bash
   docker ps | grep ecommerce-postgres
   ```

2. Check PostgreSQL logs for startup issues:
   ```bash
   docker-compose logs postgres
   ```

### Out of Memory Errors

If Spark container crashes with OOM errors:

1. Increase Docker's available memory in Docker Desktop settings
2. Reduce Spark's memory usage by adding this to spark-streaming service in docker-compose.yml:
   ```yaml
   environment:
     - SPARK_EXECUTOR_MEMORY=512m
     - SPARK_DRIVER_MEMORY=512m
   ```

## Advanced Usage

### Custom Data Generator

To use a custom data generator:

1. Mount your generator script into the container:
   ```yaml
   volumes:
     - ./my-custom-generator.py:/generator/data_generator.py
   ```

2. Restart the services:
   ```bash
   docker-compose down
   docker-compose up -d
   ```

### Database Backups

To backup the PostgreSQL database:
```bash
docker exec ecommerce-postgres pg_dump -U postgres ecommerce > backup.sql
```

### Scaling Components

For higher throughput, you can adjust the resources allocated to Spark or run multiple instances of certain services.