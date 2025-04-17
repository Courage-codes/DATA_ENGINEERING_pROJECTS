-- ===================== Phase 5: Test Queries =====================

-- Test: Stock replenishment
-- This query tests the replenish_stock function by adding 20 units to product 1.
SELECT inventory.replenish_stock(1, 20);

-- Test: Place a mixed-item order
-- This query tests the place_order function by placing an order with 3 products.
SELECT * FROM inventory.place_order(
  1,
  '[{"product_id":1,"quantity":5},{"product_id":2,"quantity":12},{"product_id":3,"quantity":7}]'
);

-- Test: Generate customer order summary report
SELECT * FROM inventory.customer_order_summary;

-- Test: Generate low stock report
SELECT * FROM inventory.low_stock_report;

-- Test: Generate customer spending summary
SELECT * FROM inventory.customer_spending_summary;
