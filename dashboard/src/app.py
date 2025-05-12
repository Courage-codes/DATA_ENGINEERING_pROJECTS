#!/usr/bin/env python3
"""
Enhanced Heart Rate Monitoring Dashboard
Provides a visually stunning real-time visualization of heart rate data
"""
import os
import dash
from dash import dcc, html
from dash.dependencies import Input, Output
import plotly.graph_objs as go
import pandas as pd
import psycopg2
from datetime import datetime, timedelta
import time
import logging
import numpy as np

# --- Logging Setup ---
LOG_DIR = '/app/logs'
os.makedirs(LOG_DIR, exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(LOG_DIR, 'dashboard.log'), mode='a'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger('dashboard')

# --- Database Config ---
DB_PARAMS = {
    'host': os.environ.get('DB_HOST', 'localhost'),
    'port': os.environ.get('DB_PORT', '5432'),
    'database': os.environ.get('DB_NAME', 'heartrate_db'),
    'user': os.environ.get('DB_USER', 'postgres'),
    'password': os.environ.get('DB_PASSWORD', 'password')
}

# --- Mock Data Generation (for testing) ---
def generate_mock_heart_rate_data(minutes=5):
    """Generate mock heart rate data for testing."""
    end_time = datetime.now()
    start_time = end_time - timedelta(minutes=minutes)
    
    # Create time series
    timestamps = pd.date_range(start=start_time, end=end_time, freq='10S')
    
    # Generate realistic heart rate data with some variation
    base_heart_rate = 75  # Typical resting heart rate
    noise = np.random.normal(0, 5, len(timestamps))  # Add some random variation
    periodic_variation = 10 * np.sin(np.linspace(0, 2*np.pi, len(timestamps)))
    
    heart_rates = base_heart_rate + noise + periodic_variation
    heart_rates = np.clip(heart_rates, 50, 120)  # Realistic range
    
    # Create DataFrame
    df = pd.DataFrame({
        'timestamp': timestamps,
        'heart_rate': heart_rates,
        'customer_id': 1  # Mock customer ID
    })
    
    return df

# --- Data Fetch Functions ---
def get_heart_rate_data(minutes=5):
    """Fetch heart rate data from the last N minutes."""
    try:
        # First, try to connect to the database
        conn = psycopg2.connect(**DB_PARAMS)
        query = f"""
            SELECT customer_id, timestamp, heart_rate
            FROM heartbeats
            WHERE timestamp >= NOW() - INTERVAL '{minutes} minutes'
            ORDER BY timestamp;
        """
        df = pd.read_sql_query(query, conn)
        conn.close()
        
        # If we get data from the database, return it
        if not df.empty:
            logger.debug(f"Fetched {len(df)} records from DB.")
            return df
    except Exception as e:
        logger.warning(f"Database fetch failed: {e}")
    
    # If no database data, generate mock data
    logger.info("Generating mock heart rate data")
    return generate_mock_heart_rate_data(minutes)

def get_heart_rate_stats(minutes=5):
    """Fetch heart rate statistics from the last N minutes."""
    df = get_heart_rate_data(minutes)
    
    stats = {
        'total_readings': len(df),
        'avg_heart_rate': round(df['heart_rate'].mean(), 1),
        'min_heart_rate': round(df['heart_rate'].min(), 1),
        'max_heart_rate': round(df['heart_rate'].max(), 1),
        'low_heart_rates': len(df[df['heart_rate'] < 50]),
        'high_heart_rates': len(df[df['heart_rate'] > 100])
    }
    
    logger.debug(f"Calculated stats: {stats}")
    return stats

# --- Dash App Setup ---
app = dash.Dash(__name__)
app.title = "HeartSync Pro Dashboard"

# Custom color palette
COLOR_PALETTE = {
    'background': '#f4f6f9',
    'primary': '#3498db',
    'secondary': '#2ecc71',
    'text_dark': '#2c3e50',
    'text_light': '#ecf0f1',
    'card_bg': '#ffffff',
    'accent': '#e74c3c'
}

# --- Enhanced Dashboard Layout ---
app.layout = html.Div([
    # Gradient Header
    html.Div([
        html.H1("HeartSync Pro", style={
            'color': COLOR_PALETTE['text_light'],
            'textAlign': 'center',
            'padding': '20px',
            'margin': '0',
            'background': f'linear-gradient(135deg, {COLOR_PALETTE["primary"]}, {COLOR_PALETTE["secondary"]})',
            'boxShadow': '0 4px 6px rgba(0,0,0,0.1)'
        }),
    ]),

    # Main Content Container
    html.Div([
        # Real-time Statistics Row
        html.Div([
            html.Div(id='stats-container', style={
                'display': 'flex',
                'justifyContent': 'space-between',
                'padding': '15px',
                'backgroundColor': COLOR_PALETTE['background'],
                'borderRadius': '10px',
                'margin': '15px 0',
                'boxShadow': '0 4px 8px rgba(0,0,0,0.05)'
            })
        ]),

        # Graphs Container (Side by Side)
        html.Div([
            # Left Column - Time Series Graph
            html.Div([
                html.H4("Heart Rate Trend", style={
                    'textAlign': 'center', 
                    'color': COLOR_PALETTE['text_dark'],
                    'marginBottom': '10px'
                }),
                dcc.Graph(id='heart-rate-graph', style={
                    'backgroundColor': COLOR_PALETTE['card_bg'],
                    'borderRadius': '10px',
                    'boxShadow': '0 4px 8px rgba(0,0,0,0.1)',
                    'padding': '10px'
                })
            ], style={'width': '50%', 'display': 'inline-block', 'verticalAlign': 'top', 'paddingRight': '10px'}),

            # Right Column - Distribution Histogram
            html.Div([
                html.H4("Heart Rate Distribution", style={
                    'textAlign': 'center', 
                    'color': COLOR_PALETTE['text_dark'],
                    'marginBottom': '10px'
                }),
                dcc.Graph(id='heart-rate-histogram', style={
                    'backgroundColor': COLOR_PALETTE['card_bg'],
                    'borderRadius': '10px',
                    'boxShadow': '0 4px 8px rgba(0,0,0,0.1)',
                    'padding': '10px'
                })
            ], style={'width': '50%', 'display': 'inline-block', 'verticalAlign': 'top', 'paddingLeft': '10px'})
        ], style={'display': 'flex', 'justifyContent': 'space-between'}),
    ], style={
        'maxWidth': '1200px', 
        'margin': 'auto', 
        'padding': '0 15px',
        'backgroundColor': COLOR_PALETTE['background']
    }),

    # Interval Component for Real-time Updates
    dcc.Interval(
        id='interval-component',
        interval=5*1000,  # Update every 5 seconds
        n_intervals=0
    )
], style={
    'fontFamily': "'Roboto', 'Segoe UI', Arial, sans-serif",
    'backgroundColor': COLOR_PALETTE['background'],
    'margin': '0',
    'padding': '0',
    'height': '100vh'
})

# --- Dash Callbacks ---
@app.callback(
    Output('stats-container', 'children'),
    Input('interval-component', 'n_intervals')
)
def update_stats(n):
    stats = get_heart_rate_stats()
    stat_style = {
        'padding': '15px',
        'margin': '0 10px',
        'borderRadius': '8px',
        'textAlign': 'center',
        'backgroundColor': COLOR_PALETTE['card_bg'],
        'boxShadow': '0 4px 8px rgba(0,0,0,0.05)',
        'flex': '1',
        'color': COLOR_PALETTE['text_dark']
    }
    return [
        html.Div([
            html.H5("Total Readings", style={'color': COLOR_PALETTE['primary'], 'marginBottom': '5px'}),
            html.H3(f"{stats['total_readings']}", style={'color': COLOR_PALETTE['text_dark']})
        ], style=stat_style),
        html.Div([
            html.H5("Average HR", style={'color': COLOR_PALETTE['secondary'], 'marginBottom': '5px'}),
            html.H3(f"{stats['avg_heart_rate']} bpm", style={'color': COLOR_PALETTE['text_dark']})
        ], style=stat_style),
        html.Div([
            html.H5("Min HR", style={'color': COLOR_PALETTE['primary'], 'marginBottom': '5px'}),
            html.H3(f"{stats['min_heart_rate']} bpm", style={'color': COLOR_PALETTE['text_dark']})
        ], style=stat_style),
        html.Div([
            html.H5("Max HR", style={'color': COLOR_PALETTE['accent'], 'marginBottom': '5px'}),
            html.H3(f"{stats['max_heart_rate']} bpm", style={'color': COLOR_PALETTE['text_dark']})
        ], style=stat_style)
    ]

@app.callback(
    Output('heart-rate-graph', 'figure'),
    Input('interval-component', 'n_intervals')
)
def update_graph(n):
    df = get_heart_rate_data()
    if df.empty:
        return {
            'data': [],
            'layout': go.Layout(
                title='No data available',
                xaxis={'title': 'Time'},
                yaxis={'title': 'Heart Rate (BPM)'},
                plot_bgcolor=COLOR_PALETTE['background'],
                paper_bgcolor=COLOR_PALETTE['background']
            )
        }
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    df_grouped = df.groupby(pd.Grouper(key='timestamp', freq='10S')).agg({'heart_rate': 'mean'}).reset_index()
    
    # Create area graph with gradient fill
    return {
        'data': [
            go.Scatter(
                x=df_grouped['timestamp'],
                y=df_grouped['heart_rate'],
                mode='lines',
                name='Average Heart Rate',
                line={'color': COLOR_PALETTE['primary'], 'width': 2},
                fill='tozeroy',  # Fill area to zero on y-axis
                fillcolor=(
                    f'rgba({int(50)}, {int(152)}, {int(219)}, 0.3)'  # Transparent blue
                ),
            )
        ],
        'layout': go.Layout(
            title='Real-time Heart Rate Trend',
            xaxis={
                'title': 'Time', 
                'gridcolor': 'rgba(0,0,0,0.05)',
                'showline': False,
                'zeroline': False
            },
            yaxis={
                'title': 'Heart Rate (BPM)', 
                'range': [40, 180], 
                'gridcolor': 'rgba(0,0,0,0.05)',
                'showline': False,
                'zeroline': False
            },
            margin={'l': 60, 'r': 40, 't': 50, 'b': 50},
            height=400,
            plot_bgcolor=COLOR_PALETTE['background'],
            paper_bgcolor=COLOR_PALETTE['background'],
            font={'color': COLOR_PALETTE['text_dark']},
            showlegend=False
        )
    }

@app.callback(
    Output('heart-rate-histogram', 'figure'),
    Input('interval-component', 'n_intervals')
)
def update_histogram(n):
    df = get_heart_rate_data()
    if df.empty:
        return {
            'data': [],
            'layout': go.Layout(
                title='No data available',
                xaxis={'title': 'Heart Rate (BPM)'},
                yaxis={'title': 'Count'},
                plot_bgcolor=COLOR_PALETTE['background'],
                paper_bgcolor=COLOR_PALETTE['background']
            )
        }
    return {
        'data': [
            go.Histogram(
                x=df['heart_rate'],
                nbinsx=20,
                marker={'color': COLOR_PALETTE['secondary'], 'line': {'color': COLOR_PALETTE['text_dark'], 'width': 1}},
                opacity=0.7
            )
        ],
        'layout': go.Layout(
            title='Heart Rate Distribution',
            xaxis={
                'title': 'Heart Rate (BPM)', 
                'range': [40, 180], 
                'gridcolor': 'rgba(0,0,0,0.05)',
                'showline': False,
                'zeroline': False
            },
            yaxis={
                'title': 'Count', 
                'gridcolor': 'rgba(0,0,0,0.05)',
                'showline': False,
                'zeroline': False
            },
            margin={'l': 60, 'r': 40, 't': 50, 'b': 50},
            height=400,
            plot_bgcolor=COLOR_PALETTE['background'],
            paper_bgcolor=COLOR_PALETTE['background'],
            font={'color': COLOR_PALETTE['text_dark']},
            showlegend=False
        )
    }

# --- Database Wait Helper ---
def wait_for_database():
    max_retries = 10
    retries = 0
    while retries < max_retries:
        try:
            conn = psycopg2.connect(**DB_PARAMS)
            conn.close()
            logger.info("Database is ready!")
            return True
        except psycopg2.OperationalError:
            retries += 1
            logger.warning(f"Database not ready, retrying... ({retries}/{max_retries})")
            time.sleep(5)
    logger.error("Could not connect to database after multiple attempts")
    return False

# --- Main Entrypoint ---
if __name__ == "__main__":
    logger.info("Starting HeartSync Pro Dashboard...")
    wait_for_database()
    app.run_server(host='0.0.0.0', port=8050, debug=False)