#!/usr/bin/env python3
"""
Dashboard for Heart Rate Monitoring System
Provides real-time visualization of heart rate data
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

# Get database connection parameters from environment variables
DB_PARAMS = {
    'host': os.environ.get('DB_HOST', 'postgres'),
    'port': os.environ.get('DB_PORT', '5432'),
    'database': os.environ.get('DB_NAME', 'heartbeat_db'),
    'user': os.environ.get('DB_USER', 'postgres'),
    'password': os.environ.get('DB_PASSWORD', 'postgres')
}

# Function to get heart rate data from PostgreSQL
def get_heart_rate_data(minutes=5):
    """Get heart rate data from the last N minutes"""
    try:
        # Connect to the database
        conn = psycopg2.connect(**DB_PARAMS)
        
        # Query for data in the last N minutes
        query = f"""
        SELECT customer_id, timestamp, heart_rate 
        FROM heartbeats 
        WHERE timestamp >= NOW() - INTERVAL '{minutes} minutes'
        ORDER BY timestamp;
        """
        
        # Load data into a pandas DataFrame
        df = pd.read_sql_query(query, conn)
        conn.close()
        
        return df
    except Exception as e:
        print(f"Error fetching heart rate data: {e}")
        return pd.DataFrame(columns=['customer_id', 'timestamp', 'heart_rate'])

# Function to get heart rate statistics
def get_heart_rate_stats(minutes=5):
    """Get heart rate statistics from the last N minutes"""
    try:
        # Connect to the database
        conn = psycopg2.connect(**DB_PARAMS)
        
        # Query for statistics
        query = f"""
        SELECT 
            COUNT(*) as total_readings,
            AVG(heart_rate) as avg_heart_rate,
            MIN(heart_rate) as min_heart_rate,
            MAX(heart_rate) as max_heart_rate,
            COUNT(CASE WHEN heart_rate < 50 THEN 1 END) as low_heart_rates,
            COUNT(CASE WHEN heart_rate > 100 THEN 1 END) as high_heart_rates
        FROM heartbeats 
        WHERE timestamp >= NOW() - INTERVAL '{minutes} minutes';
        """
        
        # Execute query
        with conn.cursor() as cursor:
            cursor.execute(query)
            result = cursor.fetchone()
            
        conn.close()
        
        if result:
            return {
                'total_readings': result[0],
                'avg_heart_rate': round(result[1], 1) if result[1] else 0,
                'min_heart_rate': result[2] if result[2] else 0,
                'max_heart_rate': result[3] if result[3] else 0,
                'low_heart_rates': result[4],
                'high_heart_rates': result[5]
            }
        else:
            return {
                'total_readings': 0,
                'avg_heart_rate': 0,
                'min_heart_rate': 0,
                'max_heart_rate': 0,
                'low_heart_rates': 0,
                'high_heart_rates': 0
            }
    except Exception as e:
        print(f"Error fetching heart rate statistics: {e}")
        return {
            'total_readings': 0,
            'avg_heart_rate': 0,
            'min_heart_rate': 0,
            'max_heart_rate': 0,
            'low_heart_rates': 0,
            'high_heart_rates': 0
        }

# Initialize the Dash app
app = dash.Dash(__name__)

app.layout = html.Div([
    html.H1("Heart Rate Monitoring Dashboard", 
            style={'textAlign': 'center', 'color': '#2c3e50'}),
    
    html.Div([
        html.Div([
            html.H4("Real-time Statistics", style={'textAlign': 'center'}),
            html.Div(id='stats-container', style={
                'display': 'flex',
                'justifyContent': 'space-around',
                'padding': '20px',
                'backgroundColor': '#f8f9fa',
                'borderRadius': '10px',
                'margin': '10px'
            })
        ], style={'width': '100%', 'marginBottom': '20px'}),
        
        html.Div([
            html.H4("Heart Rate Over Time", style={'textAlign': 'center'}),
            dcc.Graph(id='heart-rate-graph'),
        ], style={'width': '100%'}),
        
        html.Div([
            html.H4("Heart Rate Distribution", style={'textAlign': 'center'}),
            dcc.Graph(id='heart-rate-histogram')
        ], style={'width': '100%', 'marginTop': '20px'})
    ]),
    
    # Hidden div for storing the current time (for triggering updates)
    html.Div(id='time-trigger', style={'display': 'none'}),
    
    # Update interval
    dcc.Interval(
        id='interval-component',
        interval=5*1000,  # Update every 5 seconds
        n_intervals=0
    )
])

# Callback to update the time trigger
@app.callback(
    Output('time-trigger', 'children'),
    Input('interval-component', 'n_intervals')
)
def update_time(n):
    return datetime.now().strftime("%H:%M:%S")

# Callback to update statistics
@app.callback(
    Output('stats-container', 'children'),
    Input('time-trigger', 'children')
)
def update_stats(time):
    stats = get_heart_rate_stats()
    
    stat_style = {
        'padding': '10px',
        'margin': '5px',
        'borderRadius': '5px',
        'textAlign': 'center',
        'boxShadow': '0 4px 8px rgba(0,0,0,0.1)',
        'width': '120px'
    }
    
    return [
        html.Div([
            html.H5("Total"),
            html.H3(f"{stats['total_readings']}")
        ], style={**stat_style, 'backgroundColor': '#e3f2fd'}),
        
        html.Div([
            html.H5("Average"),
            html.H3(f"{stats['avg_heart_rate']} bpm")
        ], style={**stat_style, 'backgroundColor': '#e8f5e9'}),
        
        html.Div([
            html.H5("Min"),
            html.H3(f"{stats['min_heart_rate']} bpm")
        ], style={**stat_style, 'backgroundColor': '#fff8e1'}),
        
        html.Div([
            html.H5("Max"),
            html.H3(f"{stats['max_heart_rate']} bpm")
        ], style={**stat_style, 'backgroundColor': '#ffebee'}),
        
        html.Div([
            html.H5("Low HR"),
            html.H3(f"{stats['low_heart_rates']}")
        ], style={**stat_style, 'backgroundColor': '#e0f7fa'}),
        
        html.Div([
            html.H5("High HR"),
            html.H3(f"{stats['high_heart_rates']}")
        ], style={**stat_style, 'backgroundColor': '#fce4ec'})
    ]

# Callback to update the heart rate graph
@app.callback(
    Output('heart-rate-graph', 'figure'),
    Input('time-trigger', 'children')
)
def update_graph(time):
    df = get_heart_rate_data()
    
    if df.empty:
        # Return an empty figure if no data
        return {
            'data': [],
            'layout': go.Layout(
                title='No data available',
                xaxis={'title': 'Time'},
                yaxis={'title': 'Heart Rate (BPM)'}
            )
        }
    
    # Group by timestamp and calculate average heart rate
    df_grouped = df.groupby([pd.Grouper(key='timestamp', freq='10S')]).agg({
        'heart_rate': 'mean'
    }).reset_index()
    
    # Create the line chart
    figure = {
        'data': [
            go.Scatter(
                x=df_grouped['timestamp'],
                y=df_grouped['heart_rate'],
                mode='lines+markers',
                name='Average Heart Rate',
                line={'color': '#2196f3', 'width': 2},
                marker={'size': 8}
            )
        ],
        'layout': go.Layout(
            title='Average Heart Rate Over Time',
            xaxis={'title': 'Time'},
            yaxis={'title': 'Heart Rate (BPM)', 'range': [40, 180]},
            margin={'l': 60, 'r': 40, 't': 50, 'b': 50},
            height=400
        )
    }
    
    return figure

# Callback to update the heart rate histogram
@app.callback(
    Output('heart-rate-histogram', 'figure'),
    Input('time-trigger', 'children')
)
def update_histogram(time):
    df = get_heart_rate_data()
    
    if df.empty:
        # Return an empty figure if no data
        return {
            'data': [],
            'layout': go.Layout(
                title='No data available',
                xaxis={'title': 'Heart Rate (BPM)'},
                yaxis={'title': 'Count'}
            )
        }
    
    # Create the histogram
    figure = {
        'data': [
            go.Histogram(
                x=df['heart_rate'],
                nbinsx=20,
                marker={'color': '#4caf50'},
                opacity=0.7
            )
        ],
        'layout': go.Layout(
            title='Heart Rate Distribution',
            xaxis={'title': 'Heart Rate (BPM)', 'range': [40, 180]},
            yaxis={'title': 'Count'},
            margin={'l': 60, 'r': 40, 't': 50, 'b': 50},
            height=400
        )
    }
    
    return figure

# Wait for database to be ready (for containerized environment)
def wait_for_database():
    max_retries = 10
    retries = 0
    
    while retries < max_retries:
        try:
            conn = psycopg2.connect(**DB_PARAMS)
            conn.close()
            print("Database is ready!")
            return True
        except psycopg2.OperationalError:
            retries += 1
            print(f"Database not ready, retrying... ({retries}/{max_retries})")
            time.sleep(5)
    
    print("Could not connect to database after multiple attempts")
    return False

if __name__ == "__main__":
    print("Starting Heart Rate Monitoring Dashboard...")
    # Wait for database to be ready
    wait_for_database()
    # Start the Dash app
    app.run_server(host='0.0.0.0', port=8050, debug=True)
