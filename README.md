# Real-Time Customer Heart Beat Monitoring System

A dockerized microservices architecture for simulating, streaming, processing, and storing real-time heart rate data.

## System Architecture

This system consists of the following components:

- **Data Generator**: Simulates heart rate data for multiple customers
- **Kafka**: Message broker for real-time data streaming
- **Data Processor**: Consumes, validates, and stores heart rate data
- **PostgreSQL**: Database for storing validated heart rate records
- **Dashboard** : Web-based visualization of heart rate data

![Architecture Diagram](systemArchitecture.png)

## Directory Structure

```
heart-beat-monitoring-system/
├── docker-compose.yml
├── README.md
├── systemArchitecture.png
├── schema/
│   └── schema.sql
├── data-generator/
│   ├── Dockerfile
│   ├── requirements.txt
│   └── src/
│       ├── __init__.py
│       ├── data_generator.py
│       └── kafka_producer.py
├── data-processor/
│   ├── Dockerfile
│   ├── requirements.txt
│   └── src/
│       ├── __init__.py
│       ├── kafka_consumer.py
│       └── db_connector.py
└──  dashboard/
     ├── Dockerfile
     ├── Images
     ├── requirements.txt
     └── src/
         ├── __init__.py
         └── app.py
```

## Setup Instructions

### Prerequisites

- Docker and Docker Compose installed on your system
- Git (optional, for cloning the repository)

### Installation

1. Clone this repository or download the source code:

```bash
git clone <repository-url>
cd heart-beat-monitoring-system
```

2. Start the system using Docker Compose:

```bash
docker-compose up -d
```

3. Verify that all services are running:

```bash
docker-compose ps
```

### Accessing the Dashboard

The dashboard is available at: http://localhost:8050

## System Components

### Data Generator

The data generator simulates heart rate data for multiple customers. It generates:
- Random customer IDs
- Timestamp (current time)
- Heart rate values (mostly within normal range with occasional outliers)

Data is generated approximately once per second and sent to a Kafka topic.

### Kafka Message Broker

Apache Kafka serves as the real-time data streaming platform. It:
- Receives data from the generator service
- Makes it available to the processor service
- Ensures reliable message delivery
- Provides scalability and fault tolerance

### Data Processor

The data processor:
- Consumes messages from the Kafka topic
- Validates heart rate values (excludes values < 40 or > 180 bpm)
- Stores valid records in PostgreSQL
- Logs invalid records

### PostgreSQL Database

Stores validated heart rate records with the schema:
- `id`: Primary key
- `customer_id`: ID of the customer
- `timestamp`: Time of the heart rate reading
- `heart_rate`: Heart rate value in BPM

### Dashboard

A web-based dashboard built with Dash and Plotly that displays:
- Average heart rate over time
- Heart rate distribution
- Real-time statistics (total readings, average, min, max)
- Alerts for abnormal heart rates



## Stopping the System

To stop all services:

```bash
docker-compose down
```

To stop all services and remove volumes (will delete all data):

```bash
docker-compose down -v
```

## Logs and Monitoring

View logs for a specific service:

```bash
docker-compose logs -f data-generator
docker-compose logs -f data-processor
docker-compose logs -f dashboard
```

## Troubleshooting

If any service fails to start:

1. Check the logs: `docker-compose logs <service-name>`
2. Ensure all required ports are available
3. Verify that Docker has sufficient resources

