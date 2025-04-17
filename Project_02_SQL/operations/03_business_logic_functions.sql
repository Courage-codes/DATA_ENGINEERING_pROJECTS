-- ===================== Phase 2: Create Business Logic Functions =====================

-- 1) Create function to place an order
-- This function handles order placement, including stock validation and order creation.

CREATE OR REPLACE FUNCTION inventory.place_order(
  p_customer_id INT,
  p_items       JSONB
)
RETURNS TABLE(result_order_id INT, total_amount NUMERIC) AS $$
DECLARE
  v_total NUMERIC := 0;
  v_price NUMERIC;
  v_stock INT;
  v_qty   INT;
  v_item  JSONB;
  v_new_id INT;
BEGIN
  INSERT INTO inventory.orders(customer_id, total_amount)
    VALUES (p_customer_id, 0)
    RETURNING orders.order_id INTO v_new_id;

  FOR v_item IN SELECT * FROM jsonb_array_elements(p_items)
  LOOP
    SELECT price, stock_quantity
      INTO v_price, v_stock
    FROM inventory.products
    WHERE products.product_id = (v_item->>'product_id')::INT;

    v_qty := (v_item->>'quantity')::INT;

    IF v_stock < v_qty THEN
      RAISE EXCEPTION 'Not enough stock for product ID %', (v_item->>'product_id')::INT;
    END IF;

    -- Bulk‐discount logic
    IF v_qty BETWEEN 10 AND 19 THEN
      v_price := v_price * 0.95;
    ELSIF v_qty >= 20 THEN
      v_price := v_price * 0.90;
    END IF;

    INSERT INTO inventory.order_details(order_id, product_id, quantity, price)
    VALUES (v_new_id, (v_item->>'product_id')::INT, v_qty, v_price);

    v_total := v_total + v_price * v_qty;

    UPDATE inventory.products
      SET stock_quantity = products.stock_quantity - v_qty
      WHERE products.product_id = (v_item->>'product_id')::INT;

    INSERT INTO inventory.inventory_logs(product_id, change_type, quantity_changed)
    VALUES ((v_item->>'product_id')::INT, 'order', -v_qty);
  END LOOP;

  UPDATE inventory.orders
    SET total_amount = v_total
    WHERE orders.order_id = v_new_id;

  RETURN QUERY SELECT v_new_id AS result_order_id, v_total;
END;
$$ LANGUAGE plpgsql;

-- 2) Create function to replenish stock
-- This function allows for stock replenishment.

CREATE OR REPLACE FUNCTION inventory.replenish_stock(
  p_product_id INT,
  p_quantity   INT
) RETURNS VOID AS $$
BEGIN
  UPDATE inventory.products
    SET stock_quantity = stock_quantity + p_quantity
    WHERE products.product_id = p_product_id;

  INSERT INTO inventory.inventory_logs(product_id, change_type, quantity_changed)
    VALUES (p_product_id, 'replenishment', p_quantity);
END;
$$ LANGUAGE plpgsql;
