EXPLAIN ANALYZE
WITH sales_per_bp AS (
  -- Total sold quantity per branch_product_id
  SELECT
    oi.branch_product_id,
    SUM(oi.quantity) AS sold_qty
  FROM order_item oi
  GROUP BY oi.branch_product_id
),
supplier_branch_sold AS (
  -- For each (branch, supplier), compute total sold quantity that this supplier supplied
  SELECT
    bp.branch_id,
    s.supplier_id,
    SUM(sp.sold_qty) AS supplier_sold_qty
  FROM supply s
  JOIN branch_product bp
    ON bp.branch_product_id = s.branch_product_id
  JOIN sales_per_bp sp
    ON sp.branch_product_id = s.branch_product_id
  WHERE bp.branch_id IS NOT NULL
  GROUP BY bp.branch_id, s.supplier_id
),
branch_total_sold AS (
  -- Total sold quantity per branch (across all suppliers)
  SELECT
    branch_id,
    SUM(supplier_sold_qty) AS total_sold_qty
  FROM supplier_branch_sold
  GROUP BY branch_id
),
supplier_branch_time AS (
  -- Avg supply time per (branch, supplier)
  SELECT
    bp.branch_id,
    s.supplier_id,
    AVG(s.supply_time::numeric) AS supplier_avg_supply_time
  FROM supply s
  JOIN branch_product bp
    ON bp.branch_product_id = s.branch_product_id
  WHERE bp.branch_id IS NOT NULL
  GROUP BY bp.branch_id, s.supplier_id
),
branch_avg_time AS (
  -- Avg supply time per branch across all suppliers (average of all supply rows)
  SELECT
    branch_id,
    AVG(supplier_avg_supply_time) AS branch_avg_supply_time
  FROM supplier_branch_time
  GROUP BY branch_id
)
SELECT
  b.branch_id,
  b.name AS branch_name,
  sup.supplier_id,
  sup.name AS supplier_name,
  -- share of sold quantity (0..1)
  (sbs.supplier_sold_qty::numeric / NULLIF(bts.total_sold_qty, 0)) AS sold_share,
  sbt.supplier_avg_supply_time,
  bat.branch_avg_supply_time,
  -- shows WHY the supplier is optimal
  CASE
    WHEN (sbs.supplier_sold_qty::numeric / NULLIF(bts.total_sold_qty, 0)) >= 0.5
      THEN '>= 50% of sold qty'
    WHEN sbt.supplier_avg_supply_time < bat.branch_avg_supply_time
      THEN 'faster than branch avg'
  END AS optimal_reason
FROM supplier_branch_sold sbs
JOIN branch_total_sold bts
  ON bts.branch_id = sbs.branch_id
JOIN supplier_branch_time sbt
  ON sbt.branch_id = sbs.branch_id
 AND sbt.supplier_id = sbs.supplier_id
JOIN branch_avg_time bat
  ON bat.branch_id = sbs.branch_id
JOIN supplier sup
  ON sup.supplier_id = sbs.supplier_id
JOIN branch b
  ON b.branch_id = sbs.branch_id
WHERE
  -- condition A or B
  (sbs.supplier_sold_qty::numeric / NULLIF(bts.total_sold_qty, 0)) >= 0.5
  OR sbt.supplier_avg_supply_time < bat.branch_avg_supply_time
ORDER BY b.branch_id, sup.supplier_id;



-- output :
-- ->  Seq Scan on order_item oi  (cost=0.00..3321.99 rows=148699 width=12) (actual time=0.019..57.718 rows=149101.00 loops=1)"
-- Execution Time: 102.193 ms


CREATE INDEX idx_order_item_branch_product
ON order_item(branch_product_id);



EXPLAIN ANALYZE
WITH sales_per_bp AS (
  -- Total sold quantity per branch_product_id
  SELECT
    oi.branch_product_id,
    SUM(oi.quantity) AS sold_qty
  FROM order_item oi
  GROUP BY oi.branch_product_id
),
supplier_branch_sold AS (
  -- For each (branch, supplier), compute total sold quantity that this supplier supplied
  SELECT
    bp.branch_id,
    s.supplier_id,
    SUM(sp.sold_qty) AS supplier_sold_qty
  FROM supply s
  JOIN branch_product bp
    ON bp.branch_product_id = s.branch_product_id
  JOIN sales_per_bp sp
    ON sp.branch_product_id = s.branch_product_id
  WHERE bp.branch_id IS NOT NULL
  GROUP BY bp.branch_id, s.supplier_id
),
branch_total_sold AS (
  -- Total sold quantity per branch (across all suppliers)
  SELECT
    branch_id,
    SUM(supplier_sold_qty) AS total_sold_qty
  FROM supplier_branch_sold
  GROUP BY branch_id
),
supplier_branch_time AS (
  -- Avg supply time per (branch, supplier)
  SELECT
    bp.branch_id,
    s.supplier_id,
    AVG(s.supply_time::numeric) AS supplier_avg_supply_time
  FROM supply s
  JOIN branch_product bp
    ON bp.branch_product_id = s.branch_product_id
  WHERE bp.branch_id IS NOT NULL
  GROUP BY bp.branch_id, s.supplier_id
),
branch_avg_time AS (
  -- Avg supply time per branch across all suppliers (average of all supply rows)
  SELECT
    branch_id,
    AVG(supplier_avg_supply_time) AS branch_avg_supply_time
  FROM supplier_branch_time
  GROUP BY branch_id
)
SELECT
  b.branch_id,
  b.name AS branch_name,
  sup.supplier_id,
  sup.name AS supplier_name,
  -- share of sold quantity (0..1)
  (sbs.supplier_sold_qty::numeric / NULLIF(bts.total_sold_qty, 0)) AS sold_share,
  sbt.supplier_avg_supply_time,
  bat.branch_avg_supply_time,
  -- shows WHY the supplier is optimal
  CASE
    WHEN (sbs.supplier_sold_qty::numeric / NULLIF(bts.total_sold_qty, 0)) >= 0.5
      THEN '>= 50% of sold qty'
    WHEN sbt.supplier_avg_supply_time < bat.branch_avg_supply_time
      THEN 'faster than branch avg'
  END AS optimal_reason
FROM supplier_branch_sold sbs
JOIN branch_total_sold bts
  ON bts.branch_id = sbs.branch_id
JOIN supplier_branch_time sbt
  ON sbt.branch_id = sbs.branch_id
 AND sbt.supplier_id = sbs.supplier_id
JOIN branch_avg_time bat
  ON bat.branch_id = sbs.branch_id
JOIN supplier sup
  ON sup.supplier_id = sbs.supplier_id
JOIN branch b
  ON b.branch_id = sbs.branch_id
WHERE
  -- condition A or B
  (sbs.supplier_sold_qty::numeric / NULLIF(bts.total_sold_qty, 0)) >= 0.5
  OR sbt.supplier_avg_supply_time < bat.branch_avg_supply_time
ORDER BY b.branch_id, sup.supplier_id;


-- Output:
-- ->  Seq Scan on order_item oi  (cost=0.00..3326.01 rows=149101 width=12) (actual time=0.011..19.754 rows=149101.00 loops=1)"
-- Execution Time: 58.960 ms