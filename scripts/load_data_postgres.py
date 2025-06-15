import pandas as pd
from sqlalchemy import create_engine, text
import logging
import os

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def validate_dataframe(df: pd.DataFrame, required_columns: list, table_name: str):
    missing_cols = [col for col in required_columns if col not in df.columns]
    if missing_cols:
        raise ValueError(f"DataFrame for table '{table_name}' is missing columns: {missing_cols}")
    if df.empty:
        logging.warning(f"DataFrame for table '{table_name}' is empty.")

def load_data_to_postgres(data_frames_dict, db_user, db_password, db_host, db_port, db_name):
    """
    Loads processed data and KPI DataFrames into PostgreSQL using staging tables and upsert.
    """
    engine_url = f"postgresql+psycopg2://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
    engine = create_engine(engine_url)

    processed_table_env_name = os.getenv('PROCESSED_FLIGHT_DATA_TABLE', 'processed_flight_data')
    table_map = {
        'main_data': processed_table_env_name,
        'avg_fare_airline': 'kpi_avg_fare_by_airline',
        'seasonal_fare_variation': 'kpi_seasonal_fare_variation',
        'booking_count_airline': 'kpi_booking_count_by_airline',
        'popular_routes': 'kpi_popular_routes'
    }
    staging_table_map = {k: v + '_staging' for k, v in table_map.items()}

    required_columns_map = {
        'main_data': ['Airline', 'Source', 'Destination', 'Departure Date & Time', 'Base Fare (BDT)', 'Tax & Surcharge (BDT)', 'Total Fare (BDT)'],
        'avg_fare_airline': ['Airline', 'Total Fare (BDT)'],
        'seasonal_fare_variation': ['Seasonality_Label', 'Total Fare (BDT)'],
        'booking_count_airline': ['Airline', 'booking_count'],
        'popular_routes': ['Route', 'booking_count']
    }

    try:
        logging.info(f"Starting data loading into PostgreSQL database {db_name}.")

        with engine.begin() as conn:  # transaction scope

            # 1. Load data into staging tables
            for key, df in data_frames_dict.items():
                table_name = table_map.get(key)
                staging_table = staging_table_map.get(key)
                if not table_name or not staging_table:
                    logging.warning(f"No table mapping found for key '{key}'. Skipping.")
                    continue

                # Convert 'Departure Date & Time' to datetime for main_data to avoid type mismatch
                if key == 'main_data' and 'Departure Date & Time' in df.columns:
                    df['Departure Date & Time'] = pd.to_datetime(df['Departure Date & Time'], errors='coerce')

                required_cols = required_columns_map.get(key, [])
                validate_dataframe(df, required_cols, staging_table)

                if df.empty:
                    logging.warning(f"DataFrame for '{key}' (staging table {staging_table}) is empty. Truncating staging table.")
                    conn.execute(text(f'TRUNCATE TABLE IF EXISTS "{staging_table}";'))
                    continue

                logging.info(f"Loading {len(df)} rows into staging table '{staging_table}'.")
                df.to_sql(name=staging_table, con=conn, if_exists='replace', index=False, chunksize=1000)

            # 2. Upsert from staging to target tables
            upsert_statements = {
                'main_data': f"""
                    INSERT INTO {table_map['main_data']} AS target
                    SELECT * FROM {staging_table_map['main_data']}
                    ON CONFLICT ("Airline", "Source", "Destination", "Departure Date & Time") DO UPDATE SET
                      "Base Fare (BDT)" = EXCLUDED."Base Fare (BDT)",
                      "Tax & Surcharge (BDT)" = EXCLUDED."Tax & Surcharge (BDT)",
                      "Total Fare (BDT)" = EXCLUDED."Total Fare (BDT)"
                """,
                'avg_fare_airline': f"""
                    INSERT INTO {table_map['avg_fare_airline']} AS target
                    SELECT * FROM {staging_table_map['avg_fare_airline']}
                    ON CONFLICT ("Airline") DO UPDATE SET
                      "Total Fare (BDT)" = EXCLUDED."Total Fare (BDT)"
                """,
                'seasonal_fare_variation': f"""
                    INSERT INTO {table_map['seasonal_fare_variation']} AS target
                    SELECT * FROM {staging_table_map['seasonal_fare_variation']}
                    ON CONFLICT ("Seasonality_Label") DO UPDATE SET
                      "Total Fare (BDT)" = EXCLUDED."Total Fare (BDT)"
                """,
                'booking_count_airline': f"""
                    INSERT INTO {table_map['booking_count_airline']} AS target
                    SELECT * FROM {staging_table_map['booking_count_airline']}
                    ON CONFLICT ("Airline") DO UPDATE SET
                      "booking_count" = EXCLUDED."booking_count"
                """,
                'popular_routes': f"""
                    INSERT INTO {table_map['popular_routes']} AS target
                    SELECT * FROM {staging_table_map['popular_routes']}
                    ON CONFLICT ("Route") DO UPDATE SET
                      "booking_count" = EXCLUDED."booking_count"
                """
            }

            for key, upsert_sql in upsert_statements.items():
                logging.info(f"Upserting data into target table '{table_map[key]}'.")
                try:
                    conn.execute(text(upsert_sql))
                except Exception as e:
                    if key == 'main_data' and 'unique constraint' in str(e).lower():
                        raise RuntimeError(
                            f"Upsert failed for '{key}'. Please ensure a UNIQUE or PRIMARY KEY constraint exists "
                            f"on columns ('Airline', 'Source', 'Destination', 'Departure Date & Time') in the target table."
                        ) from e
                    else:
                        raise

        logging.info("All data loaded and merged successfully to PostgreSQL.")
        return True

    except Exception as e:
        logging.error(f"An error occurred during data loading to PostgreSQL: {e}", exc_info=True)
        raise
