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

CREATE OR REPLACE FUNCTION public.fn_bnpl_credit_check(
  p_customer_id   int,
  p_amount_needed numeric
)
RETURNS TABLE (
  customer_id       int,
  current_debt      numeric(14,2),
  debt_limit        numeric(14,2),
  remaining_credit  numeric(14,2),
  can_use_bnpl      boolean
)
LANGUAGE sql
STABLE
AS $$
WITH loyalty_points AS (
  SELECT
    o.customer_id,
    COALESCE(FLOOR(SUM(oi.quantity * oi.final_price_at_order_time) / 100), 0)::int AS loyalty_points
  FROM public.ordere o
  JOIN public.order_item oi
    ON oi.order_id = o.order_id
  WHERE o.customer_id = p_customer_id
    AND o.order_date >= (CURRENT_DATE - INTERVAL '3 months')
  GROUP BY o.customer_id
),
active_bnpl AS (
  SELECT
    bp.bnpl_id,
    bp.order_id,
    o.customer_id
  FROM public.bnpl_plan bp
  JOIN public.ordere o
    ON o.order_id = bp.order_id
  WHERE o.customer_id = p_customer_id
    AND bp.status = 'Active'
),
bnpl_amounts AS (
  -- Total value per BNPL (tied to that BNPL's order_id)
  SELECT
    ab.customer_id,
    ab.bnpl_id,
    COALESCE(SUM(oi.quantity * oi.final_price_at_order_time), 0)::numeric(14,2) AS bnpl_amount
  FROM active_bnpl ab
  JOIN public.order_item oi
    ON oi.order_id = ab.order_id
  GROUP BY ab.customer_id, ab.bnpl_id
),
current_debt AS (
  SELECT
    p_customer_id AS customer_id,
    COALESCE(SUM(GREATEST(ba.bnpl_amount - COALESCE(r.paid_amount, 0), 0)), 0)::numeric(14,2) AS current_debt
  FROM bnpl_amounts ba
  LEFT JOIN (
    SELECT bnpl_id, SUM(amount)::numeric(14,2) AS paid_amount
    FROM public.repayment
    GROUP BY bnpl_id
  ) r
    ON r.bnpl_id = ba.bnpl_id
)
SELECT
  p_customer_id AS customer_id,
  COALESCE(cd.current_debt, 0)::numeric(14,2) AS current_debt,
  (COALESCE(lp.loyalty_points, 0) * 20)::numeric(14,2) AS debt_limit,
  ((COALESCE(lp.loyalty_points, 0) * 20) - COALESCE(cd.current_debt, 0))::numeric(14,2) AS remaining_credit,
  (((COALESCE(lp.loyalty_points, 0) * 20) - COALESCE(cd.current_debt, 0)) >= p_amount_needed) AS can_use_bnpl
FROM current_debt cd
LEFT JOIN loyalty_points lp
  ON lp.customer_id = cd.customer_id;
$$;


-- Sample input: 100, 100000
SELECT * FROM public.fn_bnpl_credit_check(100,10000);

-- Output:
"customer_id"	"current_debt"	"debt_limit"	"remaining_credit"	"can_use_bnpl"
100	              13.51	         64700.00	        64686.49	        true