import pandas as pd
from sqlalchemy import create_engine
import logging
import os

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def get_env_season_params():
    return {
        'eid_m_d_start': list(map(int, os.getenv('PEAK_SEASON_EID_MONTH_DAY_START', '4-5').split('-'))),
        'eid_m_d_end': list(map(int, os.getenv('PEAK_SEASON_EID_MONTH_DAY_END', '4-25').split('-'))),
        'eid2_m_d_start': list(map(int, os.getenv('PEAK_SEASON_EID_2_MONTH_DAY_START', '6-5').split('-'))),
        'eid2_m_d_end': list(map(int, os.getenv('PEAK_SEASON_EID_2_MONTH_DAY_END', '6-25').split('-'))),
        'winter_m_start': int(os.getenv('PEAK_SEASON_WINTER_MONTH_START', '12')),
        'winter_d_start': int(os.getenv('PEAK_SEASON_WINTER_DAY_START', '10')),
        'winter_m_end': int(os.getenv('PEAK_SEASON_WINTER_MONTH_END', '1')),
        'winter_d_end': int(os.getenv('PEAK_SEASON_WINTER_DAY_END', '10')),
    }

def label_season(row, season_params):
    month = row['Month']
    day = row['Day']
    if (month == season_params['eid_m_d_start'][0] and day >= season_params['eid_m_d_start'][1] and day <= season_params['eid_m_d_end'][1]):
        return "Eid"
    if (month == season_params['eid2_m_d_start'][0] and day >= season_params['eid2_m_d_start'][1] and day <= season_params['eid2_m_d_end'][1]):
        return "Eid"
    if (month == season_params['winter_m_start'] and day >= season_params['winter_d_start']) or \
       (month == season_params['winter_m_end'] and day <= season_params['winter_d_end']):
        return "Winter Holidays"
    return "Regular"

def validate_and_transform_data(db_user, db_password, db_host, db_port, db_name, input_table_name, min_rows_warning=0.5):
    try:
        logging.info(f"Starting data validation and transformation from MySQL table {db_name}.{input_table_name}.")
        engine_url = f"mysql+mysqlconnector://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
        engine = create_engine(engine_url)
        query = f"SELECT * FROM {input_table_name}"
        df = pd.read_sql(query, engine)
        logging.info(f"Read {len(df)} rows from MySQL table {input_table_name}.")

        if df.empty:
            logging.warning("Input table is empty. No data to process.")
            return {name: pd.DataFrame(columns=cols) for name, cols in {
                'main_data': ['Airline', 'Source', 'Destination', 'Departure Date & Time', 'Base Fare (BDT)', 'Tax & Surcharge (BDT)', 'Total Fare (BDT)', 'Month', 'Day', 'Seasonality_Label', 'Route'],
                'avg_fare_airline': ['Airline', 'Total Fare (BDT)'],
                'seasonal_fare_variation': ['Seasonality_Label', 'Total Fare (BDT)'],
                'booking_count_airline': ['Airline', 'booking_count'],
                'popular_routes': ['Route', 'booking_count']
            }.items()}

        # --- Data Validation ---
        required_columns = ['Airline', 'Source', 'Destination', 'Base Fare (BDT)', 'Tax & Surcharge (BDT)', 'Departure Date & Time']
        df_cols_lower = {col.lower(): col for col in df.columns}
        missing_cols = []
        for req_col in required_columns:
            if req_col not in df.columns and req_col.lower() not in df_cols_lower:
                missing_cols.append(req_col)
            elif req_col.lower() in df_cols_lower and req_col not in df.columns:
                original_col_name = df_cols_lower[req_col.lower()]
                df.rename(columns={original_col_name: req_col}, inplace=True)
                logging.warning(f"Found required column '{req_col}' as '{original_col_name}'. Renamed for consistency.")

        if missing_cols:
            logging.error(f"Missing required columns: {missing_cols}")
            raise ValueError(f"Missing required columns: {missing_cols}")

        initial_rows = len(df)
        numeric_cols_to_validate = ['Base Fare (BDT)', 'Tax & Surcharge (BDT)']
        for col in numeric_cols_to_validate:
            df[col] = pd.to_numeric(df[col], errors='coerce')

        df['Departure Date & Time'] = pd.to_datetime(df['Departure Date & Time'], errors='coerce')
        cols_to_check_for_na = required_columns + numeric_cols_to_validate
        df.dropna(subset=[col for col in cols_to_check_for_na if col in df.columns], inplace=True)
        logging.info(f"Dropped {initial_rows - len(df)} rows due to missing/invalid values.")

        # Remove negative fares
        before_neg = len(df)
        df = df[(df['Base Fare (BDT)'] >= 0) & (df['Tax & Surcharge (BDT)'] >= 0)]
        logging.info(f"Filtered out {before_neg - len(df)} rows with negative base or tax fares.")

        # Remove empty categorical fields
        for col in ['Airline', 'Source', 'Destination']:
            if col in df.columns:
                before_cat = len(df)
                df[col] = df[col].astype(str).str.strip()
                df = df[df[col] != '']
                logging.info(f"Filtered out {before_cat - len(df)} rows with empty '{col}' values.")

        # Remove duplicates
        before_dupes = len(df)
        df.drop_duplicates(inplace=True)
        logging.info(f"Removed {before_dupes - len(df)} duplicate rows.")

        # Warn if too many rows were dropped
        if len(df) < min_rows_warning * initial_rows:
            logging.warning(f"More than {(1-min_rows_warning)*100:.0f}% of rows were dropped during validation.")

        if df.empty:
            logging.warning("DataFrame is empty after validation. No data to process for KPIs.")
            return {name: pd.DataFrame(columns=cols) for name, cols in {
                'main_data': ['Airline', 'Source', 'Destination', 'Departure Date & Time', 'Base Fare (BDT)', 'Tax & Surcharge (BDT)', 'Total Fare (BDT)', 'Month', 'Day', 'Seasonality_Label', 'Route'],
                'avg_fare_airline': ['Airline', 'Total Fare (BDT)'],
                'seasonal_fare_variation': ['Seasonality_Label', 'Total Fare (BDT)'],
                'booking_count_airline': ['Airline', 'booking_count'],
                'popular_routes': ['Route', 'booking_count']
            }.items()}

        # --- Data Transformation ---
        df['Total Fare (BDT)'] = (df['Base Fare (BDT)'] + df['Tax & Surcharge (BDT)']).round(2)
        df['Month'] = df['Departure Date & Time'].dt.month
        df['Day'] = df['Departure Date & Time'].dt.day
        season_params = get_env_season_params()
        df['Seasonality_Label'] = df.apply(lambda row: label_season(row, season_params), axis=1)
        df['Route'] = df['Source'] + " to " + df['Destination']

        # --- KPI Computation ---
        avg_fare_airline_df = df.groupby('Airline', as_index=False)['Total Fare (BDT)'].mean().sort_values('Total Fare (BDT)', ascending=False)
        seasonal_fare_variation_df = df.groupby('Seasonality_Label', as_index=False)['Total Fare (BDT)'].mean()
        booking_count_airline_df = df['Airline'].value_counts().reset_index()
        booking_count_airline_df.columns = ['Airline', 'booking_count']
        popular_routes_df = df['Route'].value_counts().head(10).reset_index()
        popular_routes_df.columns = ['Route', 'booking_count']

        processed_df_cols = [
            'Airline', 'Source', 'Destination', 'Departure Date & Time',
            'Base Fare (BDT)', 'Tax & Surcharge (BDT)', 'Total Fare (BDT)',
            'Month', 'Day', 'Seasonality_Label', 'Route'
        ]
        final_processed_df = df[[col for col in processed_df_cols if col in df.columns]]

        logging.info("Successfully validated, transformed data and computed KPIs.")
        logging.info(f"Final processed data row count: {len(final_processed_df)}")

        return {
            'main_data': final_processed_df,
            'avg_fare_airline': avg_fare_airline_df,
            'seasonal_fare_variation': seasonal_fare_variation_df,
            'booking_count_airline': booking_count_airline_df,
            'popular_routes': popular_routes_df
        }

    except pd.errors.DatabaseError as db_err:
        logging.error(f"Database error during validation/transformation: {db_err}")
        raise
    except ValueError as val_err:
        logging.error(f"Validation error: {val_err}")
        raise
    except Exception as e:
        logging.error(f"An error occurred during data validation/transformation: {e}", exc_info=True)
        raise

if __name__ == "__main__":
    env_path = os.path.join(os.path.dirname(__file__), '..', '.env')
    if os.path.exists(env_path):
        from dotenv import load_dotenv
        load_dotenv(dotenv_path=env_path)
        print(f"Loaded .env file from {env_path}")
    else:
        print(f".env file not found at {env_path}. Please ensure it exists for local testing.")
        os.environ.setdefault('MYSQL_USER', 'staging_user')
        os.environ.setdefault('MYSQL_PASSWORD', 'staging_password')
        os.environ.setdefault('MYSQL_HOST_FOR_AIRFLOW', 'localhost')
        os.environ.setdefault('MYSQL_PORT', '3306')
        os.environ.setdefault('MYSQL_DATABASE', 'staging_db')
        os.environ.setdefault('STAGING_TABLE_NAME', 'raw_flight_data')
        os.environ.setdefault('PEAK_SEASON_EID_MONTH_DAY_START', '4-5')
        os.environ.setdefault('PEAK_SEASON_EID_MONTH_DAY_END', '4-25')
        os.environ.setdefault('PEAK_SEASON_EID_2_MONTH_DAY_START', '6-5')
        os.environ.setdefault('PEAK_SEASON_EID_2_MONTH_DAY_END', '6-25')
        os.environ.setdefault('PEAK_SEASON_WINTER_MONTH_START', '12')
        os.environ.setdefault('PEAK_SEASON_WINTER_DAY_START', '10')
        os.environ.setdefault('PEAK_SEASON_WINTER_MONTH_END', '1')
        os.environ.setdefault('PEAK_SEASON_WINTER_DAY_END', '10')

    MYSQL_USER = os.getenv('MYSQL_USER')
    MYSQL_PASSWORD = os.getenv('MYSQL_PASSWORD')
    MYSQL_HOST = os.getenv('MYSQL_HOST_FOR_AIRFLOW', 'localhost')
    MYSQL_PORT = os.getenv('MYSQL_PORT')
    MYSQL_DATABASE = os.getenv('MYSQL_DATABASE')
    STAGING_TABLE = os.getenv('STAGING_TABLE_NAME')

    print(f"Attempting local test for validate_transform_kpi with table {MYSQL_DATABASE}.{STAGING_TABLE}")
    if not all([MYSQL_USER, MYSQL_PASSWORD, MYSQL_HOST, MYSQL_PORT, MYSQL_DATABASE, STAGING_TABLE]):
        logging.error("One or more MySQL environment variables are missing for local script execution.")
    else:
        logging.info("Attempting local test of validate_and_transform_data...")
        try:
            kpi_data_frames = validate_and_transform_data(
                db_user=MYSQL_USER,
                db_password=MYSQL_PASSWORD,
                db_host=MYSQL_HOST,
                db_port=int(MYSQL_PORT),
                db_name=MYSQL_DATABASE,
                input_table_name=STAGING_TABLE
            )
            if kpi_data_frames:
                for name, df_kpi in kpi_data_frames.items():
                    print(f"\n--- {name} --- (Top 3 rows)")
                    print(df_kpi.head(3))
            print("Local validation and transformation test completed.")
        except Exception as e:
            print(f"Local validation and transformation test failed: {e}")
