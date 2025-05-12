-- PostgreSQL schema for heart rate monitoring system

CREATE TABLE IF NOT EXISTS heartbeats (
    id SERIAL PRIMARY KEY,
    customer_id INTEGER NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    heart_rate INTEGER NOT NULL,
    CONSTRAINT unique_customer_timestamp UNIQUE (customer_id, timestamp)
);

-- Indexes for performance (optional since unique constraint creates an index)
CREATE INDEX IF NOT EXISTS idx_heartbeats_timestamp ON heartbeats(timestamp);
CREATE INDEX IF NOT EXISTS idx_heartbeats_customer_id ON heartbeats(customer_id);
