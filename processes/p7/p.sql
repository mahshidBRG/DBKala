-- Query: Common customers between two branches
-- Inputs:
--   :branch1_id (INT)  -- first branch id
--   :branch2_id (INT)  -- second branch id
-- Output:
--   customer_name,
--   orders_in_branch1,
--   orders_in_branch2,
--   branch_with_more_orders (branch name; 'Equal' if tie)
--
-- Notes:
-- - Counts DISTINCT orders per branch (not order items).
-- - Uses your post-migration schema: order_item -> branch_product_id -> branch_product.branch_id

CREATE OR REPLACE FUNCTION public.common_customers_between_branches(
  p_branch1_id int,
  p_branch2_id int
)
RETURNS TABLE (
  customer_name           varchar,
  orders_in_branch1       bigint,
  orders_in_branch2       bigint,
  branch_with_more_orders varchar
)
LANGUAGE sql
STABLE
AS $$
WITH orders_per_branch AS (
  SELECT
    o.customer_id,
    bp.branch_id,
    COUNT(DISTINCT o.order_id) AS num_orders
  FROM public.ordere o
  JOIN public.order_item oi
    ON oi.order_id = o.order_id
  JOIN public.branch_product bp
    ON bp.branch_product_id = oi.branch_product_id
  WHERE bp.branch_id IN (p_branch1_id, p_branch2_id)
  GROUP BY o.customer_id, bp.branch_id
),
common_customers AS (
  SELECT customer_id
  FROM orders_per_branch
  GROUP BY customer_id
  HAVING COUNT(DISTINCT branch_id) = 2
),
pivoted AS (
  SELECT
    cc.customer_id,
    COALESCE(MAX(CASE WHEN opb.branch_id = p_branch1_id THEN opb.num_orders END), 0) AS orders_in_branch1,
    COALESCE(MAX(CASE WHEN opb.branch_id = p_branch2_id THEN opb.num_orders END), 0) AS orders_in_branch2
  FROM common_customers cc
  JOIN orders_per_branch opb
    ON opb.customer_id = cc.customer_id
  GROUP BY cc.customer_id
)
SELECT
  c.name AS customer_name,
  p.orders_in_branch1,
  p.orders_in_branch2,
  CASE
    WHEN p.orders_in_branch1 > p.orders_in_branch2 THEN b1.name
    WHEN p.orders_in_branch2 > p.orders_in_branch1 THEN b2.name
    ELSE 'Equal'
  END AS branch_with_more_orders
FROM pivoted p
JOIN public.customer c ON c.customer_id = p.customer_id
JOIN public.branch  b1 ON b1.branch_id = p_branch1_id
JOIN public.branch  b2 ON b2.branch_id = p_branch2_id
ORDER BY customer_name;
$$;


-- Sample input: 11, 15
SELECT * FROM common_customers_between_branches(11, 15);
