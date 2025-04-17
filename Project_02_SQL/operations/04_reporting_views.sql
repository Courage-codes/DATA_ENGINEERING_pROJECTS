-- ===================== Phase 3: Reporting Views =====================

-- 1) Create order summary view
-- This view provides a summary of orders, including customer name, order date, and total amount.

CREATE OR REPLACE VIEW inventory.customer_order_summary AS
SELECT
  o.order_id,
  c.name         AS customer_name,
  o.order_date,
  o.total_amount,
  COUNT(od.order_detail_id) AS total_items
FROM inventory.orders o
JOIN inventory.customers c  ON o.customer_id = c.customer_id
JOIN inventory.order_details od ON o.order_id = od.order_id
GROUP BY o.order_id, c.name, o.order_date, o.total_amount
ORDER BY o.order_date DESC;

-- 2) Create low stock report view
-- This view shows products that are low on stock, based on the reorder level.

CREATE OR REPLACE VIEW inventory.low_stock_report AS
SELECT
  p.product_id,
  p.name,
  p.stock_quantity,
  p.reorder_level,
  (p.reorder_level - p.stock_quantity) AS units_needed
FROM inventory.products p
WHERE p.stock_quantity < p.reorder_level
ORDER BY p.stock_quantity ASC;

-- 3) Create customer spending summary view
-- This view categorizes customers based on their spending amount (Gold, Silver, Bronze).

CREATE OR REPLACE VIEW inventory.customer_spending_summary AS
SELECT
  c.customer_id,
  c.name,
  COALESCE(SUM(o.total_amount),0) AS total_spent,
  CASE
    WHEN COALESCE(SUM(o.total_amount),0) >= 1000 THEN 'Gold'
    WHEN COALESCE(SUM(o.total_amount),0) >= 500  THEN 'Silver'
    ELSE 'Bronze'
  END AS spending_tier
FROM inventory.customers c
LEFT JOIN inventory.orders o ON c.customer_id = o.customer_id
GROUP BY c.customer_id, c.name
ORDER BY total_spent DESC;
