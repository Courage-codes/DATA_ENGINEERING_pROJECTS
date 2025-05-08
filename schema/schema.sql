-- PostgreSQL schema for heart rate monitoring system

-- Create the heartbeat table
CREATE TABLE IF NOT EXISTS heartbeats (
    id SERIAL PRIMARY KEY,
    customer_id INTEGER NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    heart_rate INTEGER NOT NULL
);

-- Create an index on timestamp for faster queries
CREATE INDEX IF NOT EXISTS idx_heartbeats_timestamp ON heartbeats(timestamp);

-- Create an index on customer_id for faster lookups
CREATE INDEX IF NOT EXISTS idx_heartbeats_customer_id ON heartbeats(customer_id);
