# Flight Analysis Project

---

## Project Overview

This project implements an ETL (Extract, Transform, Load) pipeline for analyzing flight data using Apache Airflow. The pipeline processes raw flight price data from Bangladesh, transforming it into actionable Key Performance Indicators (KPIs) and analytical views stored in a PostgreSQL database.

---

## Repository Structure

```
AIRFLOW_FLIGHT_ANALYSIS/
├── config/ # Configuration files and Airflow overrides
├── dags/ # Airflow DAG definitions
│ └── flight_analysis_dag.py
├── data/ # Raw data files
│ └── Flight_Price_Dataset_of_Bangladesh.csv
├── docs/ # Documentation and diagrams
├── images/ # Project images (e.g., architecture diagrams, reports)
│ ├── AirflowPic.png
│ └── testReport.png
├── logs/ # Airflow and application logs
├── plugins/ # Custom Airflow plugins (operators, hooks, sensors)
├── reports_test/ # Generated test reports (pytest HTML reports)
│ └── report.html
├── scripts/ # Processing and helper scripts
│ ├── ingest_data.py
│ ├── init_connections.sh
│ ├── load_data_postgres.py
│ └── validate_transform_kpi.py
├── sql/ # SQL initialization and setup scripts
│ ├── mysql_setup/
│ │ └── init.sql
│ └── postgres_setup/
│ └── init.sql
├── tests/ # Unit and integration tests
│ └── test_flight_analysis_dag.py
├── .env # Environment variables configuration file
└── docker-compose.yml # Docker Compose configuration
```

---

## Pipeline Architecture

### Data Flow

- **Extract:** Raw CSV flight data is ingested into a MySQL staging database.
- **Transform:** Data is validated and transformed into KPIs.
- **Load:** Processed KPIs are loaded into PostgreSQL analytics tables.

### Component Architecture

```
                             ┌───────────────┐
                             │               │
                             │  Raw CSV Data │
                             │               │
                             └───────┬───────┘
                                     │
                                     ▼
┌─────────────────────────────────────────────────────────────────┐
│                                                                 │
│                       Airflow DAG Pipeline                      │
│                                                                 │
│  ┌─────────┐     ┌─────────────────────┐     ┌────────────────┐ │
│  │         │     │                     │     │                │ │
│  │ Ingest  ├────►│ Validate & Transform├────►│ Load to        │ │
│  │ Task    │     │ KPI Task            │     │ Postgres Task  │ │
│  │         │     │                     │     │                │ │
│  └─────────┘     └─────────────────────┘     └────────────────┘ │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
         │                    │                     │
         ▼                    ▼                     ▼
┌─────────────────┐  ┌────────────────┐  ┌───────────────────────┐
│                 │  │                │  │                       │
│ MySQL Staging   │  │ Transformed    │  │ PostgreSQL Analytics  │
│ Database        │  │ Data (XComs)   │  │ Database              │
│                 │  │                │  │                       │
└─────────────────┘  └────────────────┘  └───────────────────────┘
```

---

## Airflow DAG Description

The main DAG `flight_analysis_dag_Courage_Sena` orchestrates the ETL process with the following tasks:

| Task Name                    | Description                                             |
|-----------------------------|---------------------------------------------------------|
| **start**                   | Empty operator marking the beginning of the pipeline.   |
| **ingest_task**             | Loads raw CSV data into MySQL staging database.         |
| **validate_transform_kpi_task** | Validates and transforms data into KPIs.             |
| **load_to_postgres_task**   | Loads processed KPIs into PostgreSQL analytics database.|
| **end**                     | Empty operator marking the completion of the pipeline.  |

### Task Dependencies

```
start >> ingest_task >> validate_transform_kpi_task >> load_to_postgres_task >> end
```

---

## KPI Definitions and Computation Logic

| KPI Name              | Computation Logic                                   | Purpose                                 |
|-----------------------|----------------------------------------------------|-----------------------------------------|
| **Average Fare by Airline** | Mean ticket price grouped by airline.             | Identifies price positioning across carriers. |
| **Seasonal Fare Variation** | Average fares across different months.             | Shows price fluctuations throughout the year. |
| **Booking Count by Airline** | Total ticket sales per airline.                      | Measures market share and popularity.   |
| **Popular Routes**        | Frequency of flights between origin-destination pairs. | Identifies high-demand routes.           |
| **Main Data**             | Cleaned and validated flight data with all essential metrics. | Foundation for analytics and KPIs.      |

The KPIs are computed using a combination of SQL queries and Pandas transformations within the `validate_transform_kpi.py` script.

---

## Technical Implementation Details

### Environment Configuration

- Environment variables are stored in the `.env` file.
- Critical configurations include database connection parameters, file paths, and table names.

### Database Setup

- **MySQL:** Acts as a staging database to store raw ingested data.
- **PostgreSQL:** Serves as the analytics database for processed KPIs.

### Error Handling and Logging

- Tasks retry once on failure.
- Email notifications are sent on task failures.
- Logging is implemented using Python's `logging` module with clear, timestamped messages.

---

## Installation Guide

### Prerequisites

- **Docker:** Version 20.10 or higher.
- **Docker Compose:** Version 1.29 or higher.
- **(Optional) Python 3.8+:** For running tests outside Docker.

### Environment Variables

1. Copy `.env.example` to `.env`:

```
cp .env.example .env
```

2. Edit `.env` to configure:

- MySQL and PostgreSQL credentials.
- Database names.
- File paths for raw data.
- Table names for staging and analytics.

Below is a sample `.env` file template :

```
# --- MYSQL (Staging Database) ---
MYSQL_ROOT_PASSWORD=your_root_password
MYSQL_DATABASE=your_staging_db
MYSQL_USER=your_staging_user
MYSQL_PASSWORD=your_staging_password
MYSQL_PORT_EXTERNAL=3307
MYSQL_HOST_FOR_AIRFLOW=mysql_staging_db
MYSQL_PORT=3306

# --- POSTGRESQL (Analytics Database) ---
POSTGRES_DB=airflow
POSTGRES_USER=your_postgres_user
POSTGRES_PASSWORD=your_postgres_password
POSTGRES_PORT_EXTERNAL=5433
POSTGRES_HOST_FOR_AIRFLOW=postgres_analytics_db
POSTGRES_PORT=5432

# --- AIRFLOW ---
AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://your_postgres_user:your_postgres_password@postgres_analytics_db:5432/airflow
AIRFLOW__CORE__EXECUTOR=LocalExecutor
AIRFLOW_UID=50000
AIRFLOW_IMAGE=apache/airflow:2.9.0
AIRFLOW_WEBSERVER_PORT=8080

# --- AIRFLOW ADMIN USER ---
AIRFLOW_ADMIN_USERNAME=admin
AIRFLOW_ADMIN_FIRSTNAME=Admin
AIRFLOW_ADMIN_LASTNAME=User
AIRFLOW_ADMIN_EMAIL=your_email@example.com
AIRFLOW_ADMIN_PASSWORD=your_secure_password

# --- PROJECT SPECIFIC ---
STAGING_TABLE_NAME=raw_flight_data
RAW_DATA_FILE_PATH=/opt/airflow/data/Flight_Price_Dataset_of_Bangladesh.csv

# --- OPTIONAL: Peak Season Variables (if used in DAGs) ---
PEAK_SEASON_EID_MONTH_DAY_START=4-5
PEAK_SEASON_EID_MONTH_DAY_END=4-25
PEAK_SEASON_EID_2_MONTH_DAY_START=6-5
PEAK_SEASON_EID_2_MONTH_DAY_END=6-25
PEAK_SEASON_WINTER_MONTH_START=12
PEAK_SEASON_WINTER_DAY_START=10
PEAK_SEASON_WINTER_MONTH_END=1
PEAK_SEASON_WINTER_DAY_END=10

# --- SMTP EMAIL SETTINGS FOR AIRFLOW ---
AIRFLOW__SMTP__SMTP_HOST=smtp.gmail.com
AIRFLOW__SMTP__SMTP_STARTTLS=True
AIRFLOW__SMTP__SMTP_SSL=False
AIRFLOW__SMTP__SMTP_USER=your_email@gmail.com
AIRFLOW__SMTP__SMTP_PASSWORD=your_app_password
AIRFLOW__SMTP__SMTP_PORT=587
AIRFLOW__SMTP__SMTP_MAIL_FROM=your_email@gmail.com
```

### Database Initialization

- MySQL and PostgreSQL containers automatically run initialization scripts located in `sql/mysql_setup/init.sql` and `sql/postgres_setup/init.sql` respectively.
- Ensure databases are healthy before starting Airflow services (healthchecks included in Docker Compose).

### Starting Airflow

```
docker-compose up -d
```

- This starts all services: MySQL, PostgreSQL, Airflow scheduler, webserver, and others.
- Access the Airflow UI at [http://localhost:8080](http://localhost:8080).
- Verify scheduler and webserver logs for successful startup.

### Running Tests

```
docker compose run --rm airflow_test
```

- Runs Pytest inside the Airflow test container.
- Test reports are generated at `reports_test/report.html`.

---

## Challenges and Solutions

| Challenge                          | Solution                                                                                   |
|----------------------------------|--------------------------------------------------------------------------------------------|
| **Database Connection Management** | Flexible connection retrieval using Airflow connections with environment variable fallback. |
| **Data Validation**               | Implemented thorough validation and data quality checks in `validate_transform_kpi.py`.    |
| **Data Transfer Between Tasks**  | Used Airflow XComs with JSON serialization for efficient DataFrame transfer.               |
| **Module Import Errors**          | Mounted `scripts` folder and set `PYTHONPATH` in Docker Compose to resolve import issues.  |
| **Permission Errors on Reports** | Adjusted host folder permissions and container mounts for report generation.               |
| **Deprecated Airflow Features**  | Updated DAG to use `pendulum` for dates, replaced `schedule_interval` with `schedule`, and removed deprecated parameters. |

---

## Future Improvements

- Add data quality monitoring dashboards.
- Implement incremental data loading for scalability.
- Develop advanced KPIs (e.g., price elasticity, booking trends).
- Enhance test coverage with additional unit and integration tests.
- Automate deployment with CI/CD pipelines.
- Explore container orchestration with Kubernetes.
- Implement data lineage and audit tracking.



## Contributors

- Courage Sena (cl.kofisena@gmail.com)

---

If you have any questions or want to contribute, feel free to reach out!
