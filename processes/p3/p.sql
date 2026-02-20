CREATE OR REPLACE FUNCTION public.high_value_new_customers(
  p_today_date       date,
  p_min_orders       int,
  p_min_total_amount numeric
)
RETURNS TABLE (
  customer_id                 int,
  customer_name               varchar,
  customer_phone              varchar,
  num_orders_last_month       bigint,
  total_purchase_last_month   numeric(14,2)
)
LANGUAGE sql
STABLE
AS $$
WITH last_month_orders AS (
  SELECT
    c.customer_id,
    o.order_id
  FROM public.customer c
  JOIN public.ordere o
    ON o.customer_id = c.customer_id
  WHERE c.customer_status = 'New'
    AND o.order_date >= (p_today_date - INTERVAL '1 month')
    AND o.order_date <  (p_today_date + INTERVAL '1 day')
),
last_month_amounts AS (
  SELECT
    lmo.customer_id,
    COUNT(DISTINCT lmo.order_id) AS num_orders_last_month,
    COALESCE(SUM(oi.quantity * oi.final_price_at_order_time), 0)::numeric(14,2) AS total_purchase_last_month
  FROM last_month_orders lmo
  JOIN public.order_item oi
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
JOIN public.customer c
  ON c.customer_id = lma.customer_id
WHERE lma.num_orders_last_month > p_min_orders
  AND lma.total_purchase_last_month > p_min_total_amount
ORDER BY lma.total_purchase_last_month DESC, lma.num_orders_last_month DESC;
$$;

-- Sample input: CURRENT_DATE, 2, 1000
SELECT * FROM high_value_new_customers(CURRENT_DATE, 2, 1000);
