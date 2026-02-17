-- Query: "High-value new customers"
-- Inputs:
--   :today_date        (DATE)   -- current date (you can pass CURRENT_DATE)
--   :min_orders        (INT)    -- order-count threshold
--   :min_total_amount  (NUMERIC)-- total purchase threshold (Toman)
--
-- Output:
--   customer_name, customer_phone, number_of_orders_last_month, total_purchase_last_month
-- Definition (based on the prompt):
--   "New customers" = customers whose status is 'New'
--   Consider only orders in the last 1 month (from :today_date - 1 month to :today_date)
--   Return only those whose order count AND total purchase amount exceed the given thresholds.

WITH last_month_orders AS (
  SELECT
    c.customer_id,
    c.name  AS customer_name,
    c.phone AS customer_phone,
    o.order_id
  FROM customer c
  JOIN ordere o
    ON o.customer_id = c.customer_id
  WHERE c.customer_status = 'New'
    AND o.order_date >= (:today_date - INTERVAL '1 month')
    AND o.order_date <  (:today_date + INTERVAL '1 day')
),
last_month_amounts AS (
  SELECT
    lmo.customer_id,
    COUNT(DISTINCT lmo.order_id) AS num_orders_last_month,
    COALESCE(SUM(oi.quantity * oi.final_price_at_order_time), 0)::numeric(14,2) AS total_purchase_last_month
  FROM last_month_orders lmo
  JOIN order_item oi
    ON oi.order_id = lmo.order_id
  GROUP BY lmo.customer_id
)
SELECT
  lma.customer_id,
  c.name  AS customer_name,
  c.phone AS customer_phone,
  lma.num_orders_last_month,
  lma.total_purchase_last_month
FROM last_month_amounts lma
JOIN customer c
  ON c.customer_id = lma.customer_id
WHERE lma.num_orders_last_month > :min_orders
  AND lma.total_purchase_last_month > :min_total_amount
ORDER BY lma.total_purchase_last_month DESC, lma.num_orders_last_month DESC;
