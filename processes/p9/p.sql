-- Query #9: Customer BNPL credit check (based on loyalty points from last 3 months)
-- Inputs:
--   :customer_id   (INT)     -- the customer we want to check
--   :amount_needed (NUMERIC) -- the intended new BNPL purchase amount
--
-- Output:
--   customer_id
--   current_debt              => total unpaid BNPL amount (Active) for this customer
--   debt_limit                => BNPL limit for this customer (loyalty_points * 20)
--   remaining_credit          => debt_limit - current_debt
--   can_use_bnpl (BOOLEAN)    => TRUE if remaining_credit >= amount_needed

WITH loyalty_points AS (
  -- Fetch the loyalty points for the customer based on last 3 months' orders
  SELECT
    o.customer_id,
    FLOOR(SUM(oi.quantity * oi.final_price_at_order_time) / 100) AS loyalty_points
  FROM order_item oi
  JOIN ordere o ON oi.order_id = o.order_id
  WHERE o.customer_id = :customer_id
    AND o.order_date >= (CURRENT_DATE - INTERVAL '3 months')
  GROUP BY o.customer_id
),
active_bnpl AS (
  SELECT
    bp.bnpl_id,
    o.customer_id,
    bp.status
  FROM bnpl_plan bp
  JOIN ordere o ON o.order_id = bp.order_id
  WHERE o.customer_id = :customer_id
    AND bp.status = 'Active'
),
bnpl_amounts AS (
  -- Amount of each BNPL = total order value (sum of items: quantity * final_price_at_order_time)
  SELECT
    ab.customer_id,
    ab.bnpl_id,
    COALESCE(SUM(oi.quantity * oi.final_price_at_order_time), 0)::numeric(14,2) AS bnpl_amount
  FROM active_bnpl ab
  JOIN ordere o ON o.customer_id = ab.customer_id
  JOIN order_item oi ON oi.order_id = o.order_id
  JOIN bnpl_plan bp ON bp.order_id = o.order_id AND bp.bnpl_id = ab.bnpl_id
  GROUP BY ab.customer_id, ab.bnpl_id
),
current_debt AS (
  -- Current debt = sum(active bnpl amounts) - sum(repayments)
  SELECT
    a.customer_id,
    COALESCE(SUM(a.bnpl_amount), 0)::numeric(14,2)
    - COALESCE((
        SELECT SUM(r.amount)
        FROM repayment r
        WHERE r.bnpl_id IN (SELECT bnpl_id FROM active_bnpl)
      ), 0)::numeric(14,2) AS current_debt
  FROM bnpl_amounts a
  GROUP BY a.customer_id
)
SELECT
  lp.customer_id,
  COALESCE(cd.current_debt, 0)::numeric(14,2) AS current_debt,
  (lp.loyalty_points * 20)::numeric(14,2) AS debt_limit,
  (lp.loyalty_points * 20 - COALESCE(cd.current_debt, 0))::numeric(14,2) AS remaining_credit,
  ((lp.loyalty_points * 20 - COALESCE(cd.current_debt, 0)) >= :amount_needed) AS can_use_bnpl
FROM loyalty_points lp
LEFT JOIN current_debt cd
  ON cd.customer_id = lp.customer_id;
