-- ===================== Cleanup =====================

-- Drop all test data
-- This script is useful for cleaning up the test data after testing.
-- Drop the views and functions used for testing.

DROP VIEW IF EXISTS inventory.customer_order_summary;
DROP VIEW IF EXISTS inventory.low_stock_report;
DROP VIEW IF EXISTS inventory.customer_spending_summary;

DROP FUNCTION IF EXISTS inventory.place_order;
DROP FUNCTION IF EXISTS inventory.replenish_stock;

-- Drop the tables and schema
DROP TABLE IF EXISTS inventory.orders;
DROP TABLE IF EXISTS inventory.order_details;
DROP TABLE IF EXISTS inventory.inventory_logs;
DROP TABLE IF EXISTS inventory.products;
DROP TABLE IF EXISTS inventory.customers;

-- Drop the schema itself
DROP SCHEMA IF EXISTS inventory;
