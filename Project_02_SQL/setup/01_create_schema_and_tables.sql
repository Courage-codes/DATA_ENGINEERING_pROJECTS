-- 1. Create the database
-- This creates the 'inventory_db' database where the inventory management system will reside.
CREATE DATABASE inventory_db;

-- Switch to the newly created database
\c inventory_db;

-- 2. Create schema and tables
-- Creating the 'inventory' schema where all tables will be stored.

-- Create schema if not exists
CREATE SCHEMA IF NOT EXISTS inventory;

-- Customers Table
-- This table stores information about customers.
CREATE TABLE inventory.customers (
  customer_id    SERIAL PRIMARY KEY,        -- Unique ID for each customer
  name           VARCHAR(100) NOT NULL,     -- Full name of the customer
  email          VARCHAR(100) UNIQUE NOT NULL, -- Email address of the customer (unique)
  phone_number   VARCHAR(20)                -- Phone number of the customer
);

-- Products Table
-- This table stores details about products available in the inventory.
CREATE TABLE inventory.products (
  product_id      SERIAL PRIMARY KEY,        -- Unique ID for each product
  name            VARCHAR(100) NOT NULL,     -- Product name
  category        VARCHAR(50),               -- Category of the product
  price           NUMERIC(10,2) NOT NULL,    -- Price of the product
  stock_quantity  INT NOT NULL CHECK (stock_quantity >= 0), -- Available stock quantity (must be >= 0)
  reorder_level   INT NOT NULL CHECK (reorder_level >= 0)    -- Minimum stock level for reorder
);

-- Orders Table
-- This table stores the orders made by customers.
CREATE TABLE inventory.orders (
  order_id     SERIAL PRIMARY KEY,           -- Unique order ID
  customer_id  INT NOT NULL REFERENCES inventory.customers(customer_id), -- Foreign key to customers
  order_date   TIMESTAMP DEFAULT CURRENT_TIMESTAMP,  -- Date when the order was placed
  total_amount NUMERIC(10,2) NOT NULL CHECK (total_amount >= 0)  -- Total amount of the order
);

-- Order Details Table
-- This table stores the individual items in each order.
CREATE TABLE inventory.order_details (
  order_detail_id SERIAL PRIMARY KEY,        -- Unique ID for each order detail record
  order_id        INT NOT NULL REFERENCES inventory.orders(order_id) ON DELETE CASCADE, -- Foreign key to orders
  product_id      INT NOT NULL REFERENCES inventory.products(product_id), -- Foreign key to products
  quantity        INT NOT NULL CHECK (quantity > 0),  -- Quantity of the product in the order
  price           NUMERIC(10,2) NOT NULL CHECK (price >= 0) -- Price of the product at the time of order
);

-- Inventory Logs Table
-- This table logs all changes made to the inventory.
CREATE TABLE inventory.inventory_logs (
  log_id           SERIAL PRIMARY KEY,        -- Unique ID for each inventory log record
  product_id       INT NOT NULL REFERENCES inventory.products(product_id),  -- Foreign key to products
  change_type      VARCHAR(20) NOT NULL CHECK (change_type IN ('order','replenishment')), -- Type of inventory change
  quantity_changed INT NOT NULL,               -- Quantity changed in the inventory (positive for replenishment, negative for orders)
  change_date      TIMESTAMP DEFAULT CURRENT_TIMESTAMP -- Date when the inventory change occurred
);
