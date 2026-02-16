-- View: Marketing loyalty summary per customer
-- Includes:
-- (1) Total purchase amount (all time)
-- (2) Loyalty points computed from purchases in the last 3 months (1 point per 100 Toman)
-- (3) Membership level based on total loyalty points:
--     Bronze: points < 1000
--     Silver: 1000 <= points <= 2000
--     Gold:   points > 2000

CREATE OR REPLACE VIEW vw_marketing_customer_loyalty AS
WITH all_time_total AS (
  SELECT
    c.customer_id,
    c.name  AS customer_name,
    c.email,
    c.phone,
    COALESCE(SUM(oi.quantity * oi.final_price_at_order_time), 0)::numeric(14,2) AS total_purchase_amount_all_time
  FROM customer c
  LEFT JOIN ordere o
    ON o.customer_id = c.customer_id
  LEFT JOIN order_item oi
    ON oi.order_id = o.order_id
  GROUP BY c.customer_id, c.name, c.email, c.phone
),
last_3m_total AS (
  SELECT
    c.customer_id,
    COALESCE(SUM(oi.quantity * oi.final_price_at_order_time), 0)::numeric(14,2) AS total_purchase_amount_last_3_months
  FROM customer c
  LEFT JOIN ordere o
    ON o.customer_id = c.customer_id
   AND o.order_date >= (CURRENT_DATE - INTERVAL '3 months')
  LEFT JOIN order_item oi
    ON oi.order_id = o.order_id
  GROUP BY c.customer_id
),
points_calc AS (
  SELECT
    a.customer_id,
    a.customer_name,
    a.email,
    a.phone,
    a.total_purchase_amount_all_time,
    l.total_purchase_amount_last_3_months,
    FLOOR(l.total_purchase_amount_last_3_months / 100)::int AS loyalty_points_last_3_months
  FROM all_time_total a
  JOIN last_3m_total l
    ON l.customer_id = a.customer_id
)
SELECT
  customer_id,
  customer_name,
  email,
  phone,
  total_purchase_amount_all_time,
  loyalty_points_last_3_months,
  CASE
    WHEN loyalty_points_last_3_months < 1000 THEN 'Bronze'
    WHEN loyalty_points_last_3_months <= 2000 THEN 'Silver'
    ELSE 'Gold'
  END AS membership_level
FROM points_calc;
