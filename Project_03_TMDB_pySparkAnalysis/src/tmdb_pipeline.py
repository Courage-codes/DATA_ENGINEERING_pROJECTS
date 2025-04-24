import os
import requests
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, ArrayType, BooleanType, FloatType, LongType
from pyspark.sql.functions import col

# Constants
BASE_URL = 'https://api.themoviedb.org/3/movie/'
PARQUET_FILE = "movies_data.parquet"
MIN_NON_NULL_COLUMNS = 10
GENRE_DELIMITER = "|"
DATE_FORMAT = "yyyy-MM-dd"
BUDGET_REVENUE_SCALE = 1000000
MIN_ROI_BUDGET = 10
MIN_VOTE_COUNT = 10

# ------------------ Data Extraction Module ------------------
def load_api_key():
    """Loads the TMDB API key from environment variables."""
    return os.getenv("API_KEY")

def create_spark_session(app_name="TMDBDataFetcher"):
    """Initializes and returns a SparkSession."""
    return SparkSession.builder.appName(app_name).getOrCreate()

def fetch_movie_data(movie_id, api_key):
    """Fetches movie data from TMDB API for a given movie ID."""
    details_endpoint = f"{BASE_URL}{movie_id}?append_to_response=credits"
    details_params = {'api_key': api_key, 'language': 'en-US'}
    try:
        response = requests.get(details_endpoint, params=details_params)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Request error for movie ID {movie_id}: {e}")
        return None
    except ValueError as e:
        print(f"JSON decode error for movie ID {movie_id}: {e}")
        return None

def create_movie_schema():
    """Defines the schema for the movie data."""
    return StructType([
        StructField("adult", BooleanType(), True),
        StructField("backdrop_path", StringType(), True),
        StructField("belongs_to_collection", StructType([
            StructField("id", IntegerType(), True),
            StructField("name", StringType(), True),
            StructField("poster_path", StringType(), True),
            StructField("backdrop_path", StringType(), True)
        ]), True),
        StructField("budget", LongType(), True),
        StructField("genres", ArrayType(StructType([
            StructField("id", IntegerType(), True),
            StructField("name", StringType(), True)
        ])), True),
        StructField("homepage", StringType(), True),
        StructField("id", LongType(), True),
        StructField("imdb_id", StringType(), True),
        StructField("original_language", StringType(), True),
        StructField("original_title", StringType(), True),
        StructField("overview", StringType(), True),
        StructField("popularity", FloatType(), True),
        StructField("poster_path", StringType(), True),
        StructField("production_companies", ArrayType(StructType([
            StructField("id", IntegerType(), True),
            StructField("logo_path", StringType(), True),
            StructField("name", StringType(), True),
            StructField("origin_country", StringType(), True)
        ])), True),
        StructField("production_countries", ArrayType(StructType([
            StructField("iso_3166_1", StringType(), True),
            StructField("name", StringType(), True)
        ])), True),
        StructField("release_date", StringType(), True),
        StructField("revenue", LongType(), True),
        StructField("runtime", IntegerType(), True),
        StructField("spoken_languages", ArrayType(StructType([
            StructField("english_name", StringType(), True),
            StructField("iso_639_1", StringType(), True),
            StructField("name", StringType(), True)
        ])), True),
        StructField("status", StringType(), True),
        StructField("tagline", StringType(), True),
        StructField("title", StringType(), True),
        StructField("video", BooleanType(), True),
        StructField("vote_average", FloatType(), True),
        StructField("vote_count", LongType(), True),
        StructField("credits", StructType([
            StructField("cast", ArrayType(StructType([
                StructField("adult", BooleanType(), True),
                StructField("gender", IntegerType(), True),
                StructField("id", IntegerType(), True),
                StructField("known_for_department", StringType(), True),
                StructField("name", StringType(), True),
                StructField("original_name", StringType(), True),
                StructField("popularity", FloatType(), True),
                StructField("profile_path", StringType(), True),
                StructField("cast_id", IntegerType(), True),
                StructField("character", StringType(), True),
                StructField("credit_id", StringType(), True),
                StructField("order", IntegerType(), True)
            ])), True),
            StructField("crew", ArrayType(StructType([
                StructField("adult", BooleanType(), True),
                StructField("gender", IntegerType(), True),
                StructField("id", IntegerType(), True),
                StructField("known_for_department", StringType(), True),
                StructField("name", StringType(), True),
                StructField("original_name", StringType(), True),
                StructField("popularity", FloatType(), True),
                StructField("profile_path", StringType(), True),
                StructField("credit_id", StringType(), True),
                StructField("department", StringType(), True),
                StructField("job", StringType(), True)
            ])), True)
        ]), True)
    ])

def extract_movie_data(spark, movie_ids, api_key):
    """Extracts movie data from TMDB API and creates a Spark DataFrame."""
    movie_ids_rdd = spark.sparkContext.parallelize(movie_ids)
    movies_data_rdd = movie_ids_rdd.map(lambda movie_id: fetch_movie_data(movie_id, api_key)).filter(lambda x: x is not None)
    schema = create_movie_schema()
    return spark.createDataFrame(movies_data_rdd, schema=schema)

def save_and_load_dataframe(df, spark, file_path=PARQUET_FILE):
    """Saves the DataFrame to a Parquet file and loads it back."""
    df.coalesce(1).write.mode("overwrite").parquet(file_path)
    return spark.read.parquet(file_path)

# ------------------ Main Workflow ------------------
def main():
    """Orchestrates the movie data analysis workflow."""
    # Initialize Spark and load API key
    spark = create_spark_session()
    api_key = load_api_key()

    # Define movie IDs to fetch
    movie_ids = [
        0, 299534, 19995, 140607, 299536, 597, 135397,
        420818, 24428, 168259, 99861, 284054, 12445,
        181808, 330457, 351286, 109445, 321612, 260513
    ]

    # Data Extraction
    df = extract_movie_data(spark, movie_ids, api_key)
    df = save_and_load_dataframe(df, spark)
