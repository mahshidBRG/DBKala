-- SELECT * FROM category_association_by_name('Laptops', 3500);

SELECT category_id 
FROM category
WHERE name = 'Laptops';  -- => output:28


EXPLAIN ANALYZE
    WITH target_orders AS (
        -- Orders that include at least one product from the incoming category
        SELECT DISTINCT oi.order_id
        FROM order_item oi
        JOIN Branch_product bp
            ON bp.branch_product_id = oi.branch_product_id
        JOIN Product p
            ON p.product_id = bp.product_id
        WHERE p.category_id = 28
    )
    SELECT 
        c.category_id,
        c.name,
        COUNT(*) AS co_occurrence_count
    FROM target_orders t
    JOIN order_item oi
        ON oi.order_id = t.order_id
    JOIN Branch_product bp
        ON bp.branch_product_id = oi.branch_product_id
    JOIN Product p
        ON p.product_id = bp.product_id
    JOIN Category c
        ON c.category_id = p.category_id
    WHERE c.category_id <> 28
    GROUP BY c.category_id, c.name
    HAVING COUNT(*) >= 3500
    ORDER BY co_occurrence_count DESC;


-- Output:
-- ->  Parallel Seq Scan on order_item oi  (cost=0.00..2712.06 rows=87706 width=12) (actual time=0.008..13.430 rows=74550.50 loops=2)
--     Buffers: shared hit=29002
-- ->  Hash Join  (cost=2967.13..5909.47 rows=59606 width=8) (actual time=38.831..70.893 rows=36692.50 loops=2)"
--     Hash Cond: (oi.order_id = oi_1.order_id)"
-- Execution Time: 146.089 ms


CREATE INDEX idx_order_item_order_id
ON order_item(order_id);

CREATE INDEX idx_product_category_id
ON product(category_id);
DROP INDEX idx_product_category_id


EXPLAIN ANALYZE
    WITH target_orders AS (
        -- Orders that include at least one product from the incoming category
        SELECT DISTINCT oi.order_id
        FROM order_item oi
        JOIN Branch_product bp
            ON bp.branch_product_id = oi.branch_product_id
        JOIN Product p
            ON p.product_id = bp.product_id
        WHERE p.category_id = 28
    )
    SELECT 
        c.category_id,
        c.name,
        COUNT(*) AS co_occurrence_count
    FROM target_orders t
    JOIN order_item oi
        ON oi.order_id = t.order_id
    JOIN Branch_product bp
        ON bp.branch_product_id = oi.branch_product_id
    JOIN Product p
        ON p.product_id = bp.product_id
    JOIN Category c
        ON c.category_id = p.category_id
    WHERE c.category_id <> 28
    GROUP BY c.category_id, c.name
    HAVING COUNT(*) >= 3500
    ORDER BY co_occurrence_count DESC;




-- Output:
-- ->  Parallel Seq Scan on order_item oi  (cost=0.00..2712.06 rows=87706 width=12) (actual time=0.007..14.572 rows=74550.50 loops=2)
--      Buffers: shared hit=1835
-- Execution Time: 153.919 ms
