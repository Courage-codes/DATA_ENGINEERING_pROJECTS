-- Create a dedicated schema for the ecommerce database objects if it doesn't exist
CREATE SCHEMA IF NOT EXISTS ecommerce;

-- Create 'users' table to track unique users and their first and last seen timestamps
CREATE TABLE IF NOT EXISTS users (
    user_id VARCHAR(36) PRIMARY KEY,                -- Unique identifier for each user (UUID format)
    first_seen_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,  -- Timestamp when user was first seen
    last_seen_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP    -- Timestamp of user's most recent activity
);

-- Create 'products' table to store product details
CREATE TABLE IF NOT EXISTS products (
    product_id VARCHAR(36) PRIMARY KEY,             -- Unique identifier for each product (UUID format)
    category VARCHAR(100),                           -- Product category name
    price DECIMAL(10, 2),                            -- Price of the product with 2 decimal places
    first_seen_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP  -- Timestamp when product was first seen
);

-- Create 'sessions' table to track user sessions with metadata
CREATE TABLE IF NOT EXISTS sessions (
    session_id VARCHAR(36) PRIMARY KEY,             -- Unique identifier for each session (UUID format)
    user_id VARCHAR(36) REFERENCES users(user_id),  -- Foreign key referencing the user of the session
    ip_address VARCHAR(45),                          -- IP address of the user during the session
    user_agent TEXT,                                 -- User agent string of the browser/device
    started_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,    -- Timestamp when the session started
    last_activity_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP -- Timestamp of last activity in the session
);

-- Create 'events' table to store all e-commerce events with detailed info
CREATE TABLE IF NOT EXISTS events (
    event_id VARCHAR(36) PRIMARY KEY,                -- Unique identifier for each event (UUID format)
    event_type VARCHAR(50) NOT NULL,                  -- Type of event (e.g., purchase, view)
    user_id VARCHAR(36) NOT NULL,                      -- User who triggered the event
    product_id VARCHAR(36),                            -- Product related to the event (nullable)
    category VARCHAR(100),                             -- Product category (nullable)
    price DECIMAL(10, 2),                             -- Price of the product at event time (nullable)
    processed_timestamp TIMESTAMP NOT NULL,            -- Timestamp when event was generated/processed
    processing_time TIMESTAMP NOT NULL,                 -- Timestamp when event was processed in system
    session_id VARCHAR(36),                            -- Session during which event occurred (nullable)
    ip_address VARCHAR(45),                            -- IP address of the user (nullable)
    user_agent TEXT,                                   -- User agent string (nullable)
    search_query VARCHAR(255),                         -- Search query text (nullable)
    search_results_count INTEGER,                       -- Number of search results (nullable)
    quantity INTEGER,                                  -- Quantity purchased or involved (nullable)
    total_amount DECIMAL(10, 2),                       -- Total amount of purchase (nullable)
    payment_method VARCHAR(50)                          -- Payment method used (nullable)
);

-- Create indexes on frequently queried columns in 'events' table for performance
CREATE INDEX IF NOT EXISTS idx_events_user_id ON events(user_id);
CREATE INDEX IF NOT EXISTS idx_events_product_id ON events(product_id);
CREATE INDEX IF NOT EXISTS idx_events_event_type ON events(event_type);
CREATE INDEX IF NOT EXISTS idx_events_timestamp ON events(processed_timestamp);
CREATE INDEX IF NOT EXISTS idx_events_session ON events(session_id);

-- Define a trigger function to update reference tables after inserting into 'events'
CREATE OR REPLACE FUNCTION update_reference_tables() 
RETURNS TRIGGER AS $$
BEGIN
    -- Insert or update user information in 'users' table
    INSERT INTO users (user_id, first_seen_at, last_seen_at)
    VALUES (NEW.user_id, NEW.processed_timestamp, NEW.processed_timestamp)
    ON CONFLICT (user_id) 
    DO UPDATE SET last_seen_at = NEW.processed_timestamp;

    -- Insert product info into 'products' table if product_id is present
    IF NEW.product_id IS NOT NULL THEN
        INSERT INTO products (product_id, category, price)
        VALUES (NEW.product_id, NEW.category, NEW.price)
        ON CONFLICT (product_id) DO NOTHING;  -- Do nothing if product already exists
    END IF;

    -- Insert or update session info in 'sessions' table if session_id is present
    IF NEW.session_id IS NOT NULL THEN
        INSERT INTO sessions (session_id, user_id, ip_address, user_agent, started_at, last_activity_at)
        VALUES (NEW.session_id, NEW.user_id, NEW.ip_address, NEW.user_agent, NEW.processed_timestamp, NEW.processed_timestamp)
        ON CONFLICT (session_id) 
        DO UPDATE SET last_activity_at = NEW.processed_timestamp;
    END IF;

    -- Return the new row to complete the trigger operation
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Create a trigger on 'events' table to invoke 'update_reference_tables' after each insert
CREATE TRIGGER events_insert_trigger
AFTER INSERT ON events
FOR EACH ROW
EXECUTE FUNCTION update_reference_tables();

