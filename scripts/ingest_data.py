import os
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError, OperationalError
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def validate_dataframe(df, expected_columns, table_name):
    missing_cols = [col for col in expected_columns if col not in df.columns]
    if missing_cols:
        raise ValueError(f"CSV file is missing expected columns: {missing_cols}")
    if df[expected_columns].isnull().any().any():
        raise ValueError(f"Null values found in critical columns for table {table_name}.")
    if df.duplicated(subset=expected_columns).any():
        logging.warning(f"Duplicate rows detected based on expected columns in table {table_name}.")

def ingest_csv_to_mysql(csv_file_path, db_user, db_password, db_host, db_port, db_name, table_name, expected_columns=None, chunk_size=1000, truncate_before_load=False):
    """
    Loads data from a CSV file into a MySQL staging table.
    Supports incremental loads or full refresh (truncate + load).
    Validates schema and basic data quality.
    """
    engine = None
    try:
        logging.info(f"Starting ingestion of {csv_file_path} into MySQL table {db_name}.{table_name}.")

        if not os.path.exists(csv_file_path):
            logging.error(f"CSV file not found at {csv_file_path}.")
            raise FileNotFoundError(f"CSV file not found at {csv_file_path}")

        # Read CSV in chunks to handle large files
        chunk_iter = pd.read_csv(csv_file_path, chunksize=chunk_size)

        engine_url = f"mysql+mysqlconnector://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
        engine = create_engine(engine_url, pool_pre_ping=True)

        with engine.connect() as conn:
            if truncate_before_load:
                logging.info(f"Truncating table {table_name} before ingestion.")
                conn.execute(text(f"TRUNCATE TABLE {table_name};"))

            total_rows = 0
            for chunk in chunk_iter:
                if expected_columns:
                    validate_dataframe(chunk, expected_columns, table_name)

                # Append chunk to table
                chunk.to_sql(name=table_name, con=conn, if_exists='append', index=False)
                total_rows += len(chunk)
                logging.info(f"Ingested chunk of {len(chunk)} rows into {table_name}.")

        logging.info(f"Successfully ingested total {total_rows} rows into MySQL table {table_name}.")
        return True

    except FileNotFoundError:
        logging.error(f"Critical: CSV file not found at {csv_file_path}.")
        raise
    except pd.errors.EmptyDataError:
        logging.warning(f"Warning: CSV file {csv_file_path} is empty or unreadable as DataFrame.")
        return True
    except ValueError as ve:
        logging.error(f"Data validation error: {ve}")
        raise
    except (SQLAlchemyError, OperationalError) as db_err:
        logging.error(f"Database error during ingestion: {db_err}")
        raise
    except Exception as e:
        logging.error(f"An unexpected error occurred during data ingestion: {e}")
        raise
    finally:
        if engine:
            engine.dispose()
            logging.info("Database connection disposed.")

if __name__ == "__main__":
    required_vars = [
        'RAW_DATA_FILE_PATH',
        'MYSQL_USER',
        'MYSQL_PASSWORD',
        'MYSQL_HOST_FOR_AIRFLOW',
        'MYSQL_PORT',
        'MYSQL_DATABASE',
        'STAGING_TABLE_NAME'
    ]

    missing_vars = [var for var in required_vars if not os.getenv(var)]
    if missing_vars:
        logging.error(f"Missing required environment variables: {', '.join(missing_vars)}")
        print(f"Please provide these variables in your .env file or environment: {', '.join(missing_vars)}")
        exit(1)

    RAW_FILE_PATH = os.getenv('RAW_DATA_FILE_PATH')
    MYSQL_USER = os.getenv('MYSQL_USER')
    MYSQL_PASSWORD = os.getenv('MYSQL_PASSWORD')
    MYSQL_HOST = os.getenv('MYSQL_HOST_FOR_AIRFLOW')
    MYSQL_PORT = int(os.getenv('MYSQL_PORT'))
    MYSQL_DATABASE = os.getenv('MYSQL_DATABASE')
    STAGING_TABLE = os.getenv('STAGING_TABLE_NAME')

    expected_columns = [
        'Airline', 'Source', 'Destination', 'Departure Date & Time',
        'Base Fare (BDT)', 'Tax & Surcharge (BDT)'
    ]

    logging.info(f"Using CSV file path: {RAW_FILE_PATH}")
    logging.info(f"MySQL host: {MYSQL_HOST}, database: {MYSQL_DATABASE}, table: {STAGING_TABLE}")

    try:
        ingest_csv_to_mysql(
            csv_file_path=RAW_FILE_PATH,
            db_user=MYSQL_USER,
            db_password=MYSQL_PASSWORD,
            db_host=MYSQL_HOST,
            db_port=MYSQL_PORT,
            db_name=MYSQL_DATABASE,
            table_name=STAGING_TABLE,
            expected_columns=expected_columns,
            chunk_size=1000,
            truncate_before_load=True  # set True for full refresh, False for incremental
        )
        logging.info("Data ingestion completed successfully.")
    except Exception as e:
        logging.error(f"Data ingestion failed: {e}")
        exit(1)
