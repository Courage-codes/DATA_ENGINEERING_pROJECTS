# TMDB Movie Data Analysis

This project performs an analysis of movie data fetched from The Movie Database (TMDb) API. The main goal is to extract relevant movie information, clean, preprocess the data, and generate key insights related to movie performance (e.g., revenue, budget, ROI, etc.). The analysis also includes movie search functionalities based on specific criteria (actor, genre, director, etc.).

## 📋 Table of Contents

1. [Project Overview](#project-overview)
2. [Data Extraction](#data-extraction)
3. [Data Cleaning and Transformation](#data-cleaning-and-transformation)
4. [Data Analysis](#data-analysis)
5. [Visualizations](#visualizations)

---

## 📌 Project Overview

This project utilizes data from The Movie Database (TMDb) API to analyze key metrics such as:

- Budget
- Revenue
- Profit
- Return on Investment (ROI)
- Popularity
- Vote averages

The analysis involves ranking movies based on these metrics and exploring movie performance by franchise vs. standalone movies, as well as performing advanced searches based on criteria such as actor, genre, and director.

---

## 📥 Data Extraction

The movie data is extracted using the TMDb API, based on a list of movie IDs. Data is fetched via HTTP requests, and the script processes various details such as:

- Budget, revenue, runtime
- Cast and crew information
- Genres, production companies, and countries
- Movie performance metrics (e.g., vote averages, popularity)

The `API_KEY` for accessing the TMDb API is stored in an `.env` file to keep it secure.

---

## 🧹 Data Cleaning and Transformation

The extracted data is cleaned and transformed as follows:

1. **Irrelevant Columns Removed**: Unnecessary columns (e.g., `adult`, `imdb_id`, `video`) are dropped.
2. **Handling Nested JSON Columns**: Nested JSON columns (e.g., genres, cast, production companies) are flattened into usable string columns.
3. **Handling Missing Data**: Missing values are handled by replacing known placeholders with `NaN`, and inappropriate or impossible values (e.g., budget of 0) are cleaned.
4. **Column Type Conversion**: Columns are converted to appropriate data types (e.g., `budget`, `revenue` to numeric, `release_date` to datetime).
5. **Budget and Revenue in Million USD**: The `budget` and `revenue` columns are converted into values in million USD for better analysis.

---

## 📊 Data Analysis

The cleaned dataset is used to generate various insights, including:

- **Top 10 Movies** based on revenue, budget, profit, and ROI
- **Franchise vs. Standalone** performance analysis
- **Top Directors and Successful Franchises**
- **Advanced Searches**: Filtering movies based on actor, genre, or director combinations

The project includes functionality to rank movies and analyze their performance based on multiple key performance indicators (KPIs).

---

## 📈 Visualizations

Several data visualizations are generated to visualize trends and relationships:

- **Revenue vs. Budget**: Scatter plot showing the relationship between movie budget and revenue.
- **ROI Distribution by Genre**: Bar plot displaying the average ROI per genre.
- **Popularity vs. Rating**: Scatter plot comparing movie popularity with rating.
- **Yearly Box Office Performance**: Line chart showing yearly trends in box office revenue.

---


