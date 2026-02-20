-- Weighted average profit margin per sub-category inside an input parent category,
-- using the *snapshot cost at order time* stored on each order_item row.

CREATE OR REPLACE FUNCTION fn_weighted_profit_margin_by_subcategory(p_category_name TEXT)
RETURNS TABLE (
  sub_category_name TEXT,
  weighted_avg_profit_margin NUMERIC(12,6),
  total_quantity BIGINT
)
LANGUAGE sql
AS $$
WITH subcats AS (
  -- Sub-categories are Category rows whose parent is the input category.
  SELECT c_sub.category_id, c_sub.name
  FROM category c_parent
  JOIN category c_sub ON c_sub.parent_category_id = c_parent.category_id
  WHERE c_parent.name = p_category_name
),
lines AS (
  -- Order lines with their sub-category and both snapshots:
  -- final_price_at_order_time (sale price per unit) and cost_price_at_order_time (cost per unit).
  SELECT
    sc.name AS sub_category_name,
    oi.quantity,
    oi.final_price_at_order_time AS final_price,
    oi.cost_price_at_order_time  AS cost_price
  FROM order_item oi
  JOIN branch_product bp ON bp.branch_product_id = oi.branch_product_id
  JOIN product p ON p.product_id = bp.product_id
  JOIN subcats sc ON sc.category_id = p.category_id
  WHERE oi.quantity > 0
    AND oi.final_price_at_order_time > 0
    AND oi.cost_price_at_order_time IS NOT NULL
)
SELECT
  sub_category_name,
  (SUM( ((final_price - cost_price) / final_price) * quantity ) / SUM(quantity))::numeric(12,6)
    AS weighted_avg_profit_margin,
  SUM(quantity)::bigint AS total_quantity
FROM lines
GROUP BY sub_category_name
ORDER BY sub_category_name;
$$;

-- Sample input: 'Electronics'
SELECT * FROM fn_weighted_profit_margin_by_subcategory('Electronics');

-- Output:
"sub_category_name"	   "weighted_avg_profit_margin"	      "total_quantity"

"Cameras"	                  0.027216	                          58154
"Laptops"               	  0.605174	                          153007
"Mobile Phones"         	  0.028441	                          52520