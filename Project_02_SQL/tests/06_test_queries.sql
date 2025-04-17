-- 1) Stock replenishment test
SELECT inventory.replenish_stock(1, 20);

-- 2) Place a mixed‐item order
SELECT * FROM inventory.place_order(
  1,
  '[{"product_id":1,"quantity":5},{"product_id":2,"quantity":12},{"product_id":3,"quantity":20}]'
);

-- 3) Check updated stock
SELECT product_id, name, stock_quantity
FROM inventory.products
WHERE product_id IN (1,2,3);

-- 4) Check inventory log entries
SELECT * FROM inventory.inventory_logs
WHERE product_id IN (1,2,3)
ORDER BY change_date DESC;

-- 5) Customer order summary
SELECT * FROM inventory.customer_order_summary LIMIT 10;

-- 6) Low stock report
SELECT * FROM inventory.low_stock_report;

-- 7) Customer spending tiers
SELECT * FROM inventory.customer_spending_summary;
