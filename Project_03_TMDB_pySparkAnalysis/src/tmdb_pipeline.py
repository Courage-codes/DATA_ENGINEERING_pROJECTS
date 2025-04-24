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



# ------------------ Data Cleaning Module ------------------
def save_and_load_dataframe(df, spark, file_path=PARQUET_FILE):
    """Saves the DataFrame to a Parquet file and loads it back."""
    df.coalesce(1).write.mode("overwrite").parquet(file_path)
    return spark.read.parquet(file_path)

def drop_irrelevant_columns(df):
    """Drops irrelevant columns from the DataFrame."""
    columns_to_drop = ['adult', 'imdb_id', 'original_title', 'video', 'homepage']
    return df.drop(*columns_to_drop)

def extract_nested_columns(df):
    """Extracts nested columns into flat string representations."""
    df = df.withColumn(
        "genres",
        F.when(F.col("genres").isNull(), F.lit(np.nan))
        .otherwise(F.array_join(F.transform(F.col("genres"), lambda x: x["name"]), GENRE_DELIMITER))
    )
    df = df.withColumn(
        "belongs_to_collection",
        F.when(F.col("belongs_to_collection").isNull(), F.lit(np.nan))
        .otherwise(F.col("belongs_to_collection")["name"])
    )
    df = df.withColumn(
        "production_countries",
        F.when(F.col("production_countries").isNull(), F.lit(np.nan))
        .otherwise(F.array_join(F.transform(F.col("production_countries"), lambda x: x["name"]), GENRE_DELIMITER))
    )
    df = df.withColumn(
        "production_companies",
        F.when(F.col("production_companies").isNull(), F.lit(np.nan))
        .otherwise(F.array_join(F.transform(F.col("production_companies"), lambda x: x["name"]), GENRE_DELIMITER))
    )
    df = df.withColumn(
        "spoken_languages",
        F.when(F.col("spoken_languages").isNull(), F.lit(np.nan))
        .otherwise(F.array_join(F.transform(F.col("spoken_languages"), lambda x: x["english_name"]), GENRE_DELIMITER))
    )
    return df

def get_list_from_credits(credits_col, key):
    """Safely retrieves a list from credits column."""
    return F.when(F.col("credits").isNull(), F.array()) \
            .otherwise(F.coalesce(F.col("credits").getItem(key), F.array()))

def extract_cast_and_crew(df):
    """Extracts cast, director, producer, and crew/cast sizes from credits."""
    df = df.withColumn(
        "cast",
        F.when(get_list_from_credits("credits", "cast").isNull(), F.lit(None))
        .otherwise(F.array_join(F.transform(get_list_from_credits("credits", "cast"), lambda x: x["name"]), GENRE_DELIMITER))
    )
    df = df.withColumn(
        "director",
        F.when(get_list_from_credits("credits", "crew").isNull(), F.lit(None))
        .otherwise(
            F.array_join(
                F.transform(
                    F.filter(get_list_from_credits("credits", "crew"), lambda x: x["job"] == "Director"),
                    lambda x: x["name"]
                ),
                GENRE_DELIMITER
            )
        )
    )
    df = df.withColumn(
        "producer",
        F.when(get_list_from_credits("credits", "crew").isNull(), F.lit(None))
        .otherwise(
            F.array_join(
                F.transform(
                    F.filter(get_list_from_credits("credits", "crew"), lambda x: x["job"] == "Producer"),
                    lambda x: x["name"]
                ),
                GENRE_DELIMITER
            )
        )
    )
    df = df.withColumn(
        "crew_size",
        F.size(get_list_from_credits("credits", "crew"))
    )
    df = df.withColumn(
        "cast_size",
        F.size(get_list_from_credits("credits", "cast"))
    )
    return df

def convert_column_types(df):
    """Converts column data types to appropriate formats."""
    df = df.withColumn("budget", col("budget").cast(FloatType())) \
        .withColumn("id", col("id").cast(IntegerType())) \
        .withColumn("popularity", col("popularity").cast(FloatType()))
    df = df.withColumn("release_date", F.to_date(col("release_date"), DATE_FORMAT))
    return df

def replace_zero_with_nan(df, cols):
    """Replaces zero values in specified columns with NaN."""
    for col_name in cols:
        df = df.withColumn(col_name, F.when(col(col_name) == 0, F.lit(np.nan)).otherwise(col(col_name)))
    return df

def scale_budget_and_revenue(df):
    """Scales budget and revenue to million USD."""
    df = df.withColumn("budget_musd", col("budget") / BUDGET_REVENUE_SCALE) \
        .withColumn("revenue_musd", col("revenue") / BUDGET_REVENUE_SCALE)
    return df

def adjust_vote_average(df):
    """Sets vote average to NaN if vote count is zero."""
    df = df.withColumn(
        "vote_average_adjusted",
        F.when(col("vote_count") > 0, col("vote_average")).otherwise(F.lit(np.nan))
    )
    return df

def replace_placeholders_with_nan(df, cols, placeholder="No Data"):
    """Replaces placeholder values in specified columns with NaN."""
    for col_name in cols:
        df = df.withColumn(col_name, F.when(col(col_name) == placeholder, F.lit(np.nan)).otherwise(col(col_name)))
    return df

def clean_dataframe(df):
    """Cleans DataFrame by removing duplicates, nulls, and filtering rows."""
    df = df.dropDuplicates(['id', 'title'])
    df = df.dropna(subset=['id', 'title'])
    non_null_count_expr = sum(F.when(F.col(c).isNotNull(), 1).otherwise(0) for c in df.columns)
    df = df.withColumn("non_null_count", non_null_count_expr)
    df = df.filter(F.col("non_null_count") >= MIN_NON_NULL_COLUMNS).drop("non_null_count")
    df = df.filter(F.col("status") == "Released").drop("status")
    return df

def reorder_columns(df, desired_order):
    """Reorders DataFrame columns to specified order."""
    return df.select(desired_order)

# ------------------ KPI Calculation Module ------------------
def calculate_kpis(df):
    """Calculates key performance indicators (profit and ROI)."""
    df = df.withColumn("profit", col("revenue_musd") - col("budget_musd"))
    df = df.withColumn("roi", col("revenue_musd") / col("budget_musd"))
    return df

def rank_movies_spark(df, column, ascending=False, filter_col=None, filter_val=None):
    """Ranks movies based on a specified column with optional filtering."""
    if filter_col and filter_val:
        filtered_df = df.filter(col(filter_col) >= filter_val)
    else:
        filtered_df = df
    window_spec = Window.orderBy(F.desc(column) if not ascending else F.asc(column))
    ranked_df = filtered_df.withColumn("rank", F.rank().over(window_spec))
    return ranked_df.select("id", "title", column, "rank").limit(10)

# Individual ranking functions
def rank_highest_revenue(df):
    """Ranks movies by highest revenue."""
    print("Highest Revenue:")
    rank_movies_spark(df, 'revenue_musd').show()

def rank_highest_budget(df):
    """Ranks movies by highest budget."""
    print("\nHighest Budget:")
    rank_movies_spark(df, 'budget_musd').show()

def rank_highest_profit(df):
    """Ranks movies by highest profit."""
    print("\nHighest Profit:")
    rank_movies_spark(df, 'profit').show()

def rank_lowest_profit(df):
    """Ranks movies by lowest profit."""
    print("\nLowest Profit:")
    rank_movies_spark(df, 'profit', ascending=True).show()

def rank_highest_roi(df):
    """Ranks movies by highest ROI with budget >= 10M."""
    print("\nHighest ROI (Budget >= 10M):")
    rank_movies_spark(df, 'roi', filter_col='budget_musd', filter_val=MIN_ROI_BUDGET).show()

def rank_lowest_roi(df):
    """Ranks movies by lowest ROI with budget >= 10M."""
    print("\nLowest ROI (Budget >= 10M):")
    rank_movies_spark(df, 'roi', ascending=True, filter_col='budget_musd', filter_val=MIN_ROI_BUDGET).show()

def rank_most_voted(df):
    """Ranks movies by highest vote count."""
    print("\nMost Voted Movies:")
    rank_movies_spark(df, 'vote_count').show()

def rank_highest_rated(df):
    """Ranks movies by highest rating with votes >= 10."""
    print("\nHighest Rated Movies (Votes >= 10):")
    rank_movies_spark(df, 'vote_average', filter_col='vote_count', filter_val=MIN_VOTE_COUNT).show()

def rank_lowest_rated(df):
    """Ranks movies by lowest rating with votes >= 10."""
    print("\nLowest Rated Movies (Votes >= 10):")
    rank_movies_spark(df, 'vote_average', ascending=True, filter_col='vote_count', filter_val=MIN_VOTE_COUNT).show()

def rank_most_popular(df):
    """Ranks movies by highest popularity."""
    print("\nMost Popular Movies:")
    rank_movies_spark(df, 'popularity').show()

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

    # Data Cleaning
    df = drop_irrelevant_columns(df)
    df = extract_nested_columns(df)
    df = extract_cast_and_crew(df)
    df = convert_column_types(df)
    df = replace_zero_with_nan(df, ['budget', 'revenue', 'runtime'])
    df = scale_budget_and_revenue(df)
    df = adjust_vote_average(df)
    df = replace_placeholders_with_nan(df, ['overview', 'tagline'])
    df = clean_dataframe(df)

    # Reorder columns
    desired_order = [
        'id', 'title', 'tagline', 'release_date', 'genres', 'belongs_to_collection',
        'original_language', 'budget_musd', 'revenue_musd', 'production_companies',
        'production_countries', 'vote_count', 'vote_average', 'popularity', 'runtime',
        'overview', 'spoken_languages', 'poster_path', 'cast', 'cast_size', 'director', 'crew_size'
    ]
    df = reorder_columns(df, desired_order)

    # KPI Calculations and Rankings
    df = calculate_kpis(df)
    rank_highest_revenue(df)
    rank_highest_budget(df)
    rank_highest_profit(df)
    rank_lowest_profit(df)
    rank_highest_roi(df)
    rank_lowest_roi(df)
    rank_most_voted(df)
    rank_highest_rated(df)
    rank_lowest_rated(df)
    rank_most_popular(df)