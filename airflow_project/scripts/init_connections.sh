#!/bin/bash
set -e

: "${MYSQL_USER:?Need to set MYSQL_USER}"
: "${MYSQL_PASSWORD:?Need to set MYSQL_PASSWORD}"
: "${POSTGRES_USER:?Need to set POSTGRES_USER}"
: "${POSTGRES_PASSWORD:?Need to set POSTGRES_PASSWORD}"

echo "Adding Airflow connections..."

# Delete existing MySQL connection if it exists, then add it
airflow connections delete 'mysql_staging_conn' || true
airflow connections add 'mysql_staging_conn' \
  --conn-type mysql \
  --conn-host mysql_staging_db \
  --conn-login "$MYSQL_USER" \
  --conn-password "$MYSQL_PASSWORD" \
  --conn-schema staging_db \
  --conn-port 3306

# Delete existing Postgres connection if it exists, then add it
airflow connections delete 'postgres_analytics_conn' || true
airflow connections add 'postgres_analytics_conn' \
  --conn-type postgres \
  --conn-host postgres_analytics_db \
  --conn-login "$POSTGRES_USER" \
  --conn-password "$POSTGRES_PASSWORD" \
  --conn-schema airflow \
  --conn-port 5432

echo "Connections added successfully."
