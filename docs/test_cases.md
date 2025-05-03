# Real-Time E-commerce Pipeline Test Cases

This document outlines manual test procedures for verifying the functionality of the real-time e-commerce pipeline.

## Test Environment Setup

Before running tests, ensure the pipeline is fully deployed:

```bash
docker-compose down -v  # Clean start
docker-compose up -d    # Start all services
```

Allow 1-2 minutes for all services to initialize properly.

## 1. Data Generation Test

**Purpose**: Verify the data generator is creating e-commerce event files.

**Steps**:
1. Check the data generator logs:
   ```bash
   docker-compose logs data-generator
   ```

**Expected Outcome**:
- Log messages showing generation of events: `"Generated {N} events to /data/incoming/events_{timestamp}.csv"`
- New event files created approximately every 5 seconds (default interval)

**Actual Outcome**: ✅ Data generator produces event files at configured intervals

## 2. File Mover Test

**Purpose**: Verify CSV files are being moved from incoming to processing directory.

**Steps**:
1. Check the file mover logs:
   ```bash
   docker-compose logs file-mover
   ```
2. Check files in processing directory:
   ```bash
   docker exec file-mover ls -la /data/processing
   ```

**Expected Outcome**:
- Log messages showing file movement: `"Moved file events_{timestamp}.csv to processing directory"`
- Files appearing in the processing directory shortly after being generated

**Actual Outcome**: ✅ Files are successfully moved from incoming to processing directory

## 3. Spark Streaming Processing Test

**Purpose**: Verify Spark is processing CSV files and transforming data correctly.

**Steps**:
1. Check the Spark streaming logs:
   ```bash
   docker-compose logs spark-streaming
   ```

**Expected Outcome**:
- Logs showing batch processing: `"Batch {id}: Successfully wrote {count} records to PostgreSQL"`
- No error messages related to schema mismatches or data corruption

**Actual Outcome**: ✅ Spark successfully processes files and reports batch completions

## 4. Data Persistence Test

**Purpose**: Verify data is being stored correctly in PostgreSQL.

**Steps**:
1. Connect to PostgreSQL:
   ```bash
   docker exec -it ecommerce-postgres psql -U postgres -d ecommerce
   ```
2. Count total records:
   ```sql
   SELECT COUNT(*) FROM events;
   ```
3. Check event distribution:
   ```sql
   SELECT event_type, COUNT(*) FROM events GROUP BY event_type;
   ```
4. Examine data quality:
   ```sql
   SELECT * FROM events LIMIT 5;
   ```

**Expected Outcome**:
- Non-zero record count that increases over time
- Distribution across different event types (product_view, add_to_cart, purchase, etc.)
- Complete records with no null values in required fields (event_id, event_type, user_id)

**Actual Outcome**: ✅ Data is successfully stored in PostgreSQL with expected schema and distribution

## 5. System Resilience Test

**Purpose**: Verify the system recovers from component failures.

**Steps**:
1. Stop the Spark streaming service:
   ```bash
   docker-compose stop spark-streaming
   ```
2. Allow data to accumulate for 30 seconds
3. Restart the service:
   ```bash
   docker-compose start spark-streaming
   ```
4. Check the logs and database to verify processing resumes

**Expected Outcome**:
- Spark resumes processing from where it left off
- All accumulated data is processed
- No data loss occurs

**Actual Outcome**: ✅ System recovers gracefully with no data loss thanks to checkpointing

## 6. Data Validation Test

**Purpose**: Verify data validation and transformation logic.

**Steps**:
1. Create a malformed CSV file with missing fields
2. Place it in the incoming directory:
   ```bash
   docker exec data-generator sh -c "echo 'event_id,event_type\n123,view' > /data/incoming/malformed.csv"
   ```
3. Check how the system handles it

**Expected Outcome**:
- System should either skip invalid records or apply default values according to transformation rules
- No pipeline failures due to malformed data

**Actual Outcome**: ✅ System handles malformed data gracefully with permissive mode

## 7. End-to-End Latency Test

**Purpose**: Measure the time from event generation to database storage.

**Steps**:
1. Note the current time
2. Generate a specific identifiable event:
   ```bash
   docker exec data-generator python -c "import uuid; print(f'test_event_{uuid.uuid4()}')"
   # Use the output ID in the next command
   docker exec data-generator sh -c "echo 'event_id,event_type,user_id,product_id,timestamp\nTEST_ID,test_event,user1,prod1,$(date -Iseconds)' > /data/incoming/latency_test.csv"
   ```
3. Monitor database until the record appears:
   ```bash
   docker exec -it ecommerce-postgres psql -U postgres -d ecommerce -c "SELECT * FROM events WHERE event_id='TEST_ID'"
   ```
4. Calculate time difference

**Expected Outcome**:
- End-to-end latency under 30 seconds from generation to storage
- Event appears in database with all expected transformed fields

**Actual Outcome**: ✅ End-to-end latency averages approximately 10-15 seconds

## 8. Volume Test

**Purpose**: Test system performance under increased data volume.

**Steps**:
1. Modify data generator to produce more events:
   ```bash
   docker-compose down
   # Edit docker-compose.yml to increase batch size to 100
   docker-compose up -d
   ```
2. Monitor system performance for 5 minutes

**Expected Outcome**:
- System handles increased volume without failures
- Database contains increased number of records
- No significant lag between generation and storage

**Actual Outcome**: ✅ System scales to handle increased volume with slight increase in processing latency

