# 🧠 Inventory & Order Management System — SQL Project

## 🚀 Project Overview

This project simulates a full **Inventory and Order Management System** using PostgreSQL. It was built as a capstone for practicing **data engineering principles**, with a focus on **relational database design**, **business logic implementation**, **reporting**, and **performance optimization**.

The system models the operational flow of a business — managing customers, products, stock levels, and sales orders — using a schema-driven approach backed by clean, modular SQL. This solution is engineered for scalability, readability, and extendability.

---

## 📁 Repository Structure

```bash
Project_02_SQL/
│
├── setup/
│   ├── 01_create_schema_and_tables.sql      # Phase 1: Database, schema and table creation
│   ├── 02_insert_sample_data.sql            # Phase 2: Insert sample customers and products
│
├── operations/
│   ├── 03_business_logic_functions.sql      # Phase 3 & 4: Business logic functions and optional triggers
│   ├── 04_reporting_views.sql               # Phase 5: Views for reporting and analytics
│   ├── 05_optimizations_and_indexing.sql    # Phase 6: Performance optimization using indexes
│
├── tests/
│   └── 06_test_queries.sql                  # Validates all key system functionality
│
├── scripts/
│   └── 07_full_project_script.sql           # Combined all-in-one executable SQL script
│
└── README.md                                # Full documentation of the project
```

## Project Structure

The project is divided into several SQL scripts that serve different purposes:

1. **`setup/01_create_database_and_schema.sql`**  
   - Creates the database and the `inventory` schema.  
   - Defines the structure of the tables (customers, products, orders, order details, and inventory logs).

2. **`setup/02_insert_sample_data.sql`**  
   - Populates the `customers` and `products` tables with sample data.

3. **`operations/03_business_logic_functions.sql`**  
   - Defines business logic functions like `place_order` and `replenish_stock` for handling order placements and stock updates.

4. **`operations/04_reporting_views.sql`**  
   - Defines reporting views for customer order summaries, low stock reports, and customer spending summaries.

5. **`operations/05_optimizations_and_indexing.sql`**  
   - Implements indexing strategies and query optimization techniques to improve the performance of frequently executed operations.

6. **`tests/06_test_queries.sql`**  
   - Contains SQL queries for testing various system functions and reports.

7. **`cleanup/07_cleanup.sql`**  
   - Provides a script for cleaning up test data and dropping created tables, views, and functions.

## Setup and Installation

### Clone the repository (if applicable):
```bash
git clone <repository_url>
cd <project_directory>
```

## Prerequisites

* **PostgreSQL Installed:** Ensure you have PostgreSQL installed on your system.
* **psql Command-Line Tool:** The `psql` command-line tool should be accessible in your system's PATH.

## Setup Instructions

### Create the Database
Run the `01_create_database_and_schema.sql` script to create the database and schema, and define the necessary tables:

```bash
psql -U <username> -f setup/01_create_database_and_schema.sql
```

### Insert Sample Data
Run the `02_insert_sample_data.sql` script to populate the `customers` and `products` tables with sample data:

```bash
psql -U <username> -f setup/02_insert_sample_data.sql
```

### Define Business Logic Functions
Run the `03_business_logic_functions.sql` script to define the core business logic functions for placing orders and replenishing stock:

```bash
psql -U <username> -f operations/03_business_logic_functions.sql
```

### Create Reporting Views
Run the `04_reporting_views.sql` script to create views that generate customer order summaries, low stock reports, and customer spending summaries:

```bash
psql -U <username> -f operations/04_reporting_views.sql
```

### Apply Optimizations and Indexing
Run the `05_optimizations_and_indexing.sql` script to create indexes and apply query optimization techniques for improving database performance:

```bash
psql -U <username> -f operations/05_optimizations_and_indexing.sql
```

### Testing the Functions
You can test the functions by running the `06_test_queries.sql` script. This script includes queries for testing stock replenishment, order placement, and the reporting views:

```bash
psql -U <username> -f tests/06_test_queries.sql
```

### Cleanup (Optional)
Once testing is done, you can run the `07_cleanup.sql` script to clean up the database and drop the tables, views, and functions:

```bash
psql -U <username> -f cleanup/07_cleanup.sql
```

---

## 📊 Report Summaries

### 1. Customer Order Summary
This view provides a summary of all orders placed by customers, including customer name, order date, total amount, and total number of items in each order.

**Sample Query:**
```sql
SELECT * 
FROM inventory.customer_order_summary;
```

### 2. Low Stock Report
The low stock report highlights products that have stock quantities below the reorder level, indicating the need for replenishment.

**Sample Query:**
```sql
SELECT * 
FROM inventory.low_stock_report;
```

### 3. Customer Spending Summary
The customer spending summary categorizes customers based on their total spending into categories like Gold, Silver, and Bronze. It helps in understanding customer purchasing behavior.

**Sample Query:**
```sql
SELECT * 
FROM inventory.customer_spending_summary;
```

### 4. Detailed Stock Information (Inventory Levels)
For a more granular view of inventory, you can retrieve the stock levels of each product along with other important details.

**Sample Query:**
```sql
SELECT product_name, stock_quantity, reorder_level
FROM inventory.products
WHERE stock_quantity <= reorder_level;
```

### 5. Order Details by Customer
If you want to check the detailed orders placed by a specific customer, including the products and quantities ordered, you can use the following query.

**Sample Query:**
```sql
SELECT o.order_id, o.order_date, p.product_name, od.quantity, (od.quantity * p.price) AS total_amount
FROM inventory.orders o
JOIN inventory.order_details od ON o.order_id = od.order_id
JOIN inventory.products p ON od.product_id = p.product_id
WHERE o.customer_id = <customer_id>
ORDER BY o.order_date;
```

### 6. Total Sales per Product
This query provides insights into the total sales for each product, helping identify top sellers.

**Sample Query:**
```sql
SELECT p.product_name, SUM(od.quantity) AS total_quantity_sold, SUM(od.quantity * p.price) AS total_sales
FROM inventory.order_details od
JOIN inventory.products p ON od.product_id = p.product_id
GROUP BY p.product_name
ORDER BY total_sales DESC;
```

---

These sample queries help you monitor key metrics in your inventory and order management system, such as customer spending patterns, low stock levels, and order history. You can adjust the queries based on your specific reporting needs.
