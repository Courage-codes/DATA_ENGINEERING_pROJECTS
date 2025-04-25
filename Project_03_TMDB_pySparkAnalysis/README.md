# TMDB Movie Analysis Pipeline

## Overview

The **TMDB Movie Analysis Pipeline** is a Python-based data engineering project that fetches movie data from The Movie Database (TMDB) API, processes it using PySpark, and performs in-depth analyses to uncover insights about movie performance. The pipeline extracts data for a predefined set of movies, cleans and transforms it, calculates key performance indicators (KPIs) such as profit and return on investment (ROI), ranks movies based on metrics like revenue and ratings, conducts targeted searches (e.g., Sci-Fi Action movies starring Bruce Willis), compares franchise vs. standalone movie performance, and visualizes results using Matplotlib and Seaborn.

This project is part of a data engineering portfolio, demonstrating skills in API data extraction, big data processing with PySpark, modular code design, and data visualization. It is ideal for data analysts, movie industry professionals, or enthusiasts interested in exploring movie performance trends.

Key features include:
- **Data Extraction**: Retrieves movie details (e.g., budget, revenue, cast, genres) from the TMDB API.
- **Data Cleaning**: Processes nested structures, handles missing data, and standardizes formats using PySpark.
- **Analysis**: 
  - Ranks movies by revenue, budget, profit, ROI, ratings, vote count, and popularity.
  - Searches for specific movies (e.g., Uma Thurman movies directed by Quentin Tarantino).
  - Compares franchise vs. standalone movie performance.
- **Visualization**: Generates plots for revenue vs. budget, ROI by genre, popularity vs. rating, yearly box office trends, and franchise vs. standalone comparisons using violin and strip plots.

## Project Structure

```
Project_03_TMDB_pySparkAnalysis/
├── notebook/
│   └── analyze_movies.ipynb
├── README.md
├── src/
│   ├── tmdb_pipeline.py        # Main script containing the analysis pipeline
│   ├── movies_data.parquet     # Output Parquet file generated during execution
```

- **`src/tmdb_pipeline.py`**: The core script with modular functions for data extraction, cleaning, KPI calculations, rankings, searches, and visualizations.
- **`src/movies_data.parquet`**: A temporary Parquet file storing cleaned movie data, generated during script execution.
- **`notebook/`**: Directory for Jupyter notebooks, likely used for exploratory analysis or documentation (contents not specified).
- **`README.md`**: This file, providing project documentation.

## Requirements

To run the project, ensure you have the following installed:
- **Python 3.8+**
- **PySpark** (`pyspark>=3.4.0`): For data processing and analysis.
- **Pandas** (`pandas>=2.0.0`): For data manipulation in visualizations.
- **NumPy** (`numpy>=1.24.0`): For numerical operations.
- **Matplotlib** (`matplotlib>=3.7.0`): For plotting.
- **Seaborn** (`seaborn>=0.12.0`): For enhanced visualizations.
- **Requests** (`requests>=2.28.0`): For API calls to TMDB.

Install dependencies using pip, preferably in a virtual environment:
```bash
pip install pyspark pandas numpy matplotlib seaborn requests
```

Additional requirements:
- A **TMDB API key**. Sign up at [TMDB](https://www.themoviedb.org/) to obtain an API key.
- **Java 8 or later**: Required for PySpark (ensure `JAVA_HOME` is set).

## Setup

1. **Clone the Repository**:
   ```bash
   git clone https://github.com/<your-username>/Project_03_TMDB_pySparkAnalysis.git
   cd Project_03_TMDB_pySparkAnalysis
   ```

2. **Create and Activate a Virtual Environment** (recommended):
   ```bash
   python -m venv pyspark_env
   source pyspark_env/bin/activate  # On Windows: pyspark_env\Scripts\activate
   ```

3. **Install Dependencies**:
   ```bash
   pip install -r requirements.txt
   ```
   If no `requirements.txt` exists, install packages manually:
   ```bash
   pip install pyspark pandas numpy matplotlib seaborn requests
   ```

4. **Set Up the TMDB API Key**:
   - Store your TMDB API key as an environment variable named `API_KEY`.
   - On Unix/Linux/MacOS:
     ```bash
     export API_KEY='your-api-key-here'
     ```
   - On Windows (Command Prompt):
     ```bash
     set API_KEY=your-api-key-here
     ```
   - For persistence, add to your shell profile (e.g., `~/.bashrc`, `~/.zshrc`):
     ```bash
     echo "export API_KEY='your-api-key-here'" >> ~/.bashrc
     source ~/.bashrc
     ```

5. **Verify Java Installation**:
   - Ensure Java is installed and configured:
     ```bash
     java -version
     ```
   - If not installed, download Java from [AdoptOpenJDK](https://adoptopenjdk.net/) or another provider and set `JAVA_HOME`.

## Usage

1. **Navigate to the `src` Directory**:
   ```bash
   cd src
   ```

2. **Run the Script**:
   ```bash
   python tmdb_pipeline.py
   ```
   The script executes the entire pipeline:
   - Fetches data for a predefined list of 19 movie IDs from the TMDB API.
   - Cleans and transforms data using PySpark (stores intermediate results in `movies_data.parquet`).
   - Calculates KPIs (profit, ROI).
   - Displays rankings for:
     - Highest/lowest revenue, budget, profit, ROI.
     - Highest/lowest ratings (with minimum vote count of 10).
     - Most voted and most popular movies.
   - Performs searches for:
     - Sci-Fi Action movies starring Bruce Willis.
     - Uma Thurman movies directed by Quentin Tarantino.
   - Compares franchise vs. standalone movie performance.
   - Generates visualizations (displayed interactively).

3. **Expected Output**:
   - **Console Output**: Tables showing ranked movies, search results, and franchise vs. standalone metrics, printed via PySpark’s `show()` method.
   - **Visualizations**: Plots displayed using Matplotlib/Seaborn:
     - Scatter plots: Revenue vs. budget, popularity vs. rating.
     - Box plot: ROI by genre.
     - Line plot: Yearly box office trends.
     - Violin plots with strip plots: Franchise vs. standalone revenue and rating distributions, annotated with sample sizes.
   - **Parquet File**: `movies_data.parquet` is updated with cleaned data.

4. **Customization**:
   - **Change Movie IDs**: Edit the `movie_ids` list in the `main()` function of `tmdb_pipeline.py` to analyze different movies.
   - **Adjust Parameters**: Modify constants (e.g., `MIN_ROI_BUDGET=10`, `MIN_VOTE_COUNT=10`) to alter filtering criteria.
   - **Extend Functionality**: Add new ranking, search, or visualization functions using the modular structure.

## Example Output

### Console (Sample Rankings)
```
Highest Revenue:
+-------+--------------------+------------+----+
|     id|               title|revenue_musd|rank|
+-------+--------------------+------------+----+
| 299534|Avengers: Endgame   |  2797.8005 |   1|
|  19995|Avatar              |  2787.965  |   2|
|    597|Titanic             |  2187.4638 |   3|
+-------+--------------------+------------+----+

Search: Sci-Fi Action movies with Bruce Willis
+--------------------+--------------------+--------------------+------------+----------+
|               title|              genres|                cast|vote_average|vote_count|
+--------------------+--------------------+--------------------+------------+----------+
|The Fifth Element   |Action|Adventure|...|Bruce Willis|Luc ...|       7.5  |     9500 |
+--------------------+--------------------+--------------------+------------+----------+
```

### Visualizations
- **Franchise vs. Standalone Revenue**: Violin plot with black dots for individual movies, annotated with sample sizes (e.g., `n=5` for franchises).
- **Yearly Box Office Trends**: Line plot showing total revenue by release year.

## Functionality Details

### Data Extraction
- Fetches movie data (budget, revenue, genres, cast, crew, etc.) from the TMDB API for a predefined list of movie IDs.
- Creates a PySpark DataFrame with a structured schema.

### Data Cleaning
- Drops irrelevant columns (e.g., `adult`, `imdb_id`).
- Flattens nested fields (e.g., genres, cast) into delimited strings.
- Converts data types, replaces zeros with NaN, scales budget/revenue to millions USD, and filters incomplete or unreleased movies.

### Analysis
- **KPIs**: Computes profit (`revenue_musd - budget_musd`) and ROI (`revenue_musd / budget_musd`).
- **Rankings**: Top 10 movies by various metrics, with filters (e.g., minimum budget for ROI, minimum votes for ratings).
- **Searches**:
  - Sci-Fi Action movies with Bruce Willis, sorted by rating and vote count.
  - Uma Thurman movies directed by Quentin Tarantino, sorted by runtime.
- **Franchise vs. Standalone**:
  - Compares mean/median metrics (revenue, ROI, budget, popularity, ratings).
  - Analyzes franchise performance (e.g., total revenue, number of movies).

### Visualization
- Uses Matplotlib and Seaborn for:
  - **Revenue vs. Budget**: Scatter plot of financial trends.
  - **ROI by Genre**: Box plot of ROI across genres.
  - **Popularity vs. Rating**: Scatter plot of audience metrics.
  - **Yearly Box Office Trends**: Line plot of revenue by year.
  - **Franchise vs. Standalone**: Box plots for revenue and ratings, showing distributions and individual data points.

