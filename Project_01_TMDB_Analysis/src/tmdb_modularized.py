import requests
import pandas as pd
import time
import os
import json
import numpy as np
import matplotlib.pyplot as plt
from dotenv import load_dotenv

def load_api_key():
    """Loads the TMDb API key from the .env file."""
    load_dotenv()
    return os.getenv("TMDB_API_KEY")

def fetch_movie_data(api_key, movie_ids):
    """Fetches movie data from the TMDb API for a list of movie IDs."""
    base_url = 'https://api.themoviedb.org/3/movie/'
    movies_data = []
    for movie_id in movie_ids:
        details_endpoint = f"{base_url}{movie_id}?append_to_response=credits"
        details_params = {
            'api_key': api_key,
            'language': 'en-US'
        }
        try:
            details_response = requests.get(details_endpoint, params=details_params)
            details_response.raise_for_status()
            data = details_response.json()
            movies_data.append(data)
            print(f"✔️ Fetched data for movie ID: {movie_id}")
        except requests.exceptions.RequestException as e:
            print(f"❌ Request error for movie ID {movie_id}: {e}")
        except ValueError as e:
            print(f"❌ JSON decode error for movie ID {movie_id}: {e}")
        time.sleep(0.2)
    return pd.DataFrame(movies_data)

def drop_irrelevant_columns(df):
    """Drops irrelevant columns from the DataFrame."""
    columns_to_drop = ['adult', 'imdb_id', 'original_title', 'video', 'homepage']
    df.drop(columns=columns_to_drop, axis=1, inplace=True)
    return df

def handle_json_columns(df):
    """Extracts relevant information from nested JSON columns."""
    df['genres'] = df['genres'].apply(lambda x: "|".join([d['name'] for d in x]) if isinstance(x, list) else np.nan)
    df['belongs_to_collection'] = df['belongs_to_collection'].apply(lambda x: x['name'] if isinstance(x, dict) else np.nan)
    df['production_countries'] = df['production_countries'].apply(lambda x: "|".join([d['name'] for d in x]) if isinstance(x, list) else np.nan)
    df['production_companies'] = df['production_companies'].apply(lambda x: "|".join([d['name'] for d in x]) if isinstance(x, list) else np.nan)
    df['spoken_languages'] = df['spoken_languages'].apply(lambda x: "|".join([d['english_name'] for d in x]) if isinstance(x, list) else np.nan)
    df['cast'] = df['credits'].apply(lambda x: '|'.join([d['name'] for d in x.get('cast', []) if isinstance(d, dict) and 'name' in d]) if isinstance(x, dict) else None)
    df['director'] = df['credits'].apply(lambda x: '|'.join([d['name'] for d in x.get('crew', []) if isinstance(d, dict) and d.get('job') == 'Director']) if isinstance(x, dict) else None)
    df['producer'] = df['credits'].apply(lambda x: '|'.join([d['name'] for d in x.get('crew', []) if isinstance(d, dict) and d.get('job') == 'Producer']) if isinstance(x, dict) else None)
    df['crew_size'] = df['credits'].apply(lambda x: len(x.get('crew', [])) if isinstance(x, dict) else 0)
    df['cast_size'] = df['credits'].apply(lambda x: len(x.get('cast', []) if isinstance(x, dict) else 0))
    return df

def convert_data_types(df):
    """Converts column data types and cleans up the data."""
    df['budget'] = pd.to_numeric(df['budget'], errors='coerce').replace(0, np.nan)
    df['id'] = pd.to_numeric(df['id'], errors='coerce')
    df['popularity'] = pd.to_numeric(df['popularity'], errors='coerce')
    df['release_date'] = pd.to_datetime(df['release_date'], errors='coerce')
    df['revenue'] = df['revenue'].replace(0, np.nan)
    df['runtime'] = df['runtime'].replace(0, np.nan)
    df['budget_musd'] = df['budget'] / 1_000_000
    df['revenue_musd'] = df['revenue'] / 1_000_000
    df['vote_average_adjusted'] = df['vote_average'].where(df['vote_count'] > 0, np.nan)
    df['overview'] = df['overview'].replace('No Data', np.nan)
    df['tagline'] = df['tagline'].replace('No Data', np.nan)
    return df

def final_cleaning_and_filtering(df):
    """Removes duplicates, filters released movies, and reorders columns."""
    df.drop_duplicates(subset=['id', 'title'], inplace=True)
    df.dropna(subset=['id', 'title'], inplace=True)
    df = df[df.notna().sum(axis=1) >= 10]
    df = df[df['status'] == 'Released'].drop(columns=['status'])
    desired_order = ['id', 'title', 'tagline', 'release_date', 'genres', 'belongs_to_collection',
                     'original_language', 'budget_musd', 'revenue_musd', 'production_companies',
                     'production_countries', 'vote_count', 'vote_average', 'popularity', 'runtime',
                     'overview', 'spoken_languages', 'poster_path', 'cast', 'cast_size', 'director', 'crew_size']
    return df[desired_order]

def rank_movies(df, column, ascending=False, filter_col=None, filter_val=None):
    """Ranks movies based on a specified column, with optional filtering."""
    if filter_col and filter_val:
        filtered_df = df[df[filter_col] >= filter_val]
    else:
        filtered_df = df
    ranked_df = filtered_df.sort_values(by=column, ascending=ascending)
    ranked_df['rank'] = ranked_df[column].rank(ascending=ascending, method='first')
    return ranked_df[['id', 'title', column, 'rank']].head(10)

def advanced_movie_searches(df):
    """Performs advanced searches based on genre, cast, and director."""
    search1_results = df[
        df['genres'].str.contains('Science Fiction', na=False) &
        df['genres'].str.contains('Action', na=False) &
        df['cast'].str.contains('Bruce Willis', na=False)
    ].sort_values(by=['vote_average', 'vote_count'], ascending=[False, False])
    print("\nSearch 1 Results:")
    print(search1_results[['title', 'genres', 'cast', 'vote_average', 'vote_count']])

    search2_results = df[
        df['cast'].str.contains('Uma Thurman', na=False) &
        df['director'].str.contains('Quentin Tarantino', na=False)
    ].sort_values(by='runtime')
    print("\nSearch 2 Results:")
    print(search2_results[['title', 'cast', 'director', 'runtime']])

def franchise_vs_standalone_analysis(df):
    """Compares the performance of franchise and standalone movies."""
    df['franchise_status'] = df['belongs_to_collection'].notna().map({True: 'Franchise', False: 'Standalone'})
    metrics = ['revenue_musd', 'roi', 'budget_musd', 'popularity', 'vote_average']
    franchise_comparison = df.groupby('franchise_status')[metrics].agg(['mean', 'median'])
    franchise_comparison.columns = ['_'.join([metric, stat]) for metric, stat in franchise_comparison.columns]
    print("\nFranchise vs. Standalone Movie Performance Comparison:")
    print(franchise_comparison)

def successful_franchises_directors(df):
    """Identifies the most successful franchises and directors."""
    franchise_performance = df[df['belongs_to_collection'].notna()].groupby('belongs_to_collection').agg(
        num_movies=('id', 'count'),
        total_budget=('budget_musd', 'sum'),
        mean_budget=('budget_musd', 'mean'),
        total_revenue=('revenue_musd', 'sum'),
        mean_revenue=('revenue_musd', 'mean'),
        mean_rating=('vote_average', 'mean')
    ).sort_values(by=['num_movies', 'total_revenue'], ascending=[False, False])
    print("\nMost Successful Movie Franchises:")
    print(franchise_performance)

    director_performance = df.groupby('director').agg(
        num_movies=('id', 'count'),
        total_revenue=('revenue_musd', 'sum'),
        mean_rating=('vote_average', 'mean')
    ).sort_values(by=['num_movies', 'total_revenue'], ascending=[False, False])
    print("\nMost Successful Directors:")
    print(director_performance)

def data_visualization(df):
    """Visualizes key aspects of the movie data."""
    plt.figure(figsize=(8, 6))
    plt.scatter(df['budget_musd'], df['revenue_musd'], alpha=0.5)
    plt.title('Revenue vs. Budget Trends')
    plt.xlabel('Budget (Million USD)')
    plt.ylabel('Revenue (Million USD)')
    plt.grid(True)
    plt.show()

    df_genre_exploded = df.copy()
    df_genre_exploded['genres'] = df_genre_exploded['genres'].str.split('|')
    df_genre_exploded = df_genre_exploded.explode('genres')
    avg_roi_by_genre = df_genre_exploded.groupby('genres')['roi'].mean().sort_values(ascending=False)
    plt.figure(figsize=(12, 6))
    plt.bar(avg_roi_by_genre.index, avg_roi_by_genre.values, color='steelblue')
    plt.title('Average ROI per Genre')
    plt.xlabel('Genre')
    plt.ylabel('Average ROI')
    plt.xticks(rotation=90)
    plt.tight_layout()
    plt.show()

    plt.figure(figsize=(8, 6))
    plt.scatter(df['popularity'], df['vote_average'], alpha=0.5)
    plt.title('Popularity vs. Rating')
    plt.xlabel('Popularity')
    plt.ylabel('Rating')
    plt.grid(True)
    plt.show()

    yearly_performance = df.groupby(pd.to_datetime(df['release_date']).dt.year)['revenue_musd'].sum()
    plt.figure(figsize=(10, 6))
    yearly_performance.plot(kind='line')
    plt.title('Yearly Trends in Box Office Performance')
    plt.xlabel('Year')
    plt.ylabel('Total Revenue (Million USD)')
    plt.grid(True)
    plt.show()

    metrics = ['Revenue (M USD)', 'ROI', 'Budget (M USD)', 'Popularity']
    franchise_values = [df[df['franchise_status'] == 'Franchise']['revenue_musd'].mean(),
                        df[df['franchise_status'] == 'Franchise']['roi'].mean(),
                        df[df['franchise_status'] == 'Franchise']['budget_musd'].mean(),
                        df[df['franchise_status'] == 'Franchise']['popularity'].mean()]
    standalone_values = [df[df['franchise_status'] == 'Standalone']['revenue_musd'].mean(),
                         df[df['franchise_status'] == 'Standalone']['roi'].mean(),
                         df[df['franchise_status'] == 'Standalone']['budget_musd'].mean(),
                         df[df['franchise_status'] == 'Standalone']['popularity'].mean()]

    x = range(len(metrics))
    bar_width = 0.35

    fig, ax = plt.subplots(figsize=(10, 6))
    ax.bar([i - bar_width/2 for i in x], franchise_values, width=bar_width, label='Franchise', color='#1f77b4')
    ax.bar([i + bar_width/2 for i in x], standalone_values, width=bar_width, label='Standalone', color='#ff7f0e')
    ax.set_xticks(x)
    ax.set_xticklabels(metrics, fontsize=12)
    ax.set_ylabel('Mean Value', fontsize=12)
    ax.set_title('Franchise vs. Standalone: Mean Performance Metrics', fontsize=14)
    ax.legend()
    ax.grid(True, axis='y', linestyle='--', alpha=0.7)
    plt.tight_layout()
    plt.show()

if __name__ == "__main__":
    api_key = load_api_key()
    if not api_key:
        print("Error: TMDb API key not found. Please set the TMDB_API_KEY environment variable.")
    else:
        movie_ids = [
            0, 299534, 19995, 140607, 299536, 597, 135397,
            420818, 24428, 168259, 99861, 284054, 12445,
            181808, 330457, 351286, 109445, 321612, 260513
        ]
        df_movies = fetch_movie_data(api_key, movie_ids)
        if not df_movies.empty:
            df_movies2 = df_movies.copy(deep=True)
            df_movies2 = drop_irrelevant_columns(df_movies2)
            df_movies2 = handle_json_columns(df_movies2)
            df_movies2 = convert_data_types(df_movies2)
            df_movies3 = final_cleaning_and_filtering(df_movies2.copy())

            print("\nMovies with the best Revenue:")
            print(rank_movies(df_movies3, 'revenue_musd'))

            print("\nHighest Budget:")
            print(rank_movies(df_movies3, 'budget_musd'))

            df_movies3['profit'] = df_movies3['revenue_musd'] - df_movies3['budget_musd']
            print("\nHighest Profit:")
            print(rank_movies(df_movies3, 'profit'))

            print("\nLowest Profit:")
            print(rank_movies(df_movies3, 'profit', ascending=True))

            df_movies3['roi'] = df_movies3['revenue_musd'] / df_movies3['budget_musd']
            print("\nHighest ROI (Budget >= 10M):")
            print(rank_movies(df_movies3, 'roi', filter_col='budget_musd', filter_val=10))

            print("\nLowest ROI (Budget >= 10M):")
            print(rank_movies(df_movies3, 'roi', ascending=True, filter_col='budget_musd', filter_val=10))

            print("\nMost Voted Movies:")
            print(rank_movies(df_movies3, 'vote_count'))

            print("\nHighest Rated Movies (Votes >= 10):")
            print(rank_movies(df_movies3, 'vote_average', filter_col='vote_count', filter_val=10))

            print("\nLowest Rated Movies (Votes >= 10):")
            print(rank_movies(df_movies3, 'vote_average', ascending=True, filter_col='vote_count', filter_val=10))

            print("\nMost Popular Movies:")
            print(rank_movies(df_movies3, 'popularity'))

            advanced_movie_searches(df_movies3.copy())
            df_movies3['is_franchise'] = df_movies3['belongs_to_collection'].notna()
            franchise_vs_standalone = df_movies3.groupby('is_franchise')[
                ['revenue_musd', 'roi', 'budget_musd', 'popularity', 'vote_average']
            ].agg(['mean', 'median'])
            franchise_vs_standalone.columns = ['_'.join(col) for col in franchise_vs_standalone.columns]
            print("\nFranchise vs. Standalone Movie Performance:")
            print(franchise_vs_standalone)

            successful_franchises_directors(df_movies3.copy())

            # Call the data visualization function to display all the plots
            data_visualization(df_movies3.copy())