-- Index to speed up lookups on customer_id for reporting
CREATE INDEX IF NOT EXISTS idx_orders_customer_id
ON inventory.orders (customer_id);

-- Index for product_id in order_details for joins and aggregation
CREATE INDEX IF NOT EXISTS idx_order_details_product_id
ON inventory.order_details (product_id);

-- Index for stock-based filtering on products
CREATE INDEX IF NOT EXISTS idx_products_stock_quantity
ON inventory.products (stock_quantity);

-- Composite index for frequent filtering on reorder-level
CREATE INDEX IF NOT EXISTS idx_products_stock_reorder
ON inventory.products (stock_quantity, reorder_level);


