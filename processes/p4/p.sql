--  4- This operation extracts orders containing at least one product from the given category 
--     and identifies other categories appearing in the same orders.
--     The co-occurrence frequency is calculated, and only categories exceeding the specified minimum threshold are returned.

-- If the name of the category is input
CREATE OR REPLACE FUNCTION category_association_by_name(
    p_category_name VARCHAR,
    p_min_count INT
)
RETURNS TABLE (
    related_category_id INT,
    related_category_name VARCHAR,
    co_occurrence_count BIGINT
)
AS $$
DECLARE
    v_category_id INT;
BEGIN
    -- find category_id
    SELECT category_id
    INTO v_category_id
    FROM Category
    WHERE name = p_category_name;

    IF v_category_id IS NULL THEN
        RAISE EXCEPTION 'Category not found';
    END IF;

    RETURN QUERY
    WITH target_orders AS (
        -- Orders that include at least one product from the incoming category
        SELECT DISTINCT oi.order_id
        FROM order_item oi
        JOIN Branch_product bp
            ON bp.branch_product_id = oi.branch_product_id
        JOIN Product p
            ON p.product_id = bp.product_id
        WHERE p.category_id = v_category_id
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
    WHERE c.category_id <> v_category_id
    GROUP BY c.category_id, c.name
    HAVING COUNT(*) >= p_min_count
    ORDER BY co_occurrence_count DESC;

END;
$$ LANGUAGE plpgsql;


-- If the id of the category is input

-- CREATE OR REPLACE FUNCTION category_association(
--     p_category_id INT,
--     p_min_count INT
-- )
-- RETURNS TABLE (
--     related_category_id INT,
--     related_category_name VARCHAR,
--     co_occurrence_count BIGINT
-- )
-- AS $$
-- BEGIN
--     RETURN QUERY
--     WITH target_orders AS (
--         -- Orders that include at least one product from the incoming category
--         SELECT DISTINCT oi.order_id
--         FROM order_item oi
--         JOIN Branch_product bp
--             ON bp.branch_product_id = oi.branch_product_id
--         JOIN Product p
--             ON p.product_id = bp.product_id
--         WHERE p.category_id = p_category_id
--     )
--     SELECT 
--         c.category_id,
--         c.name,
--         COUNT(*) AS co_occurrence_count
--     FROM target_orders t
--     JOIN order_item oi
--         ON oi.order_id = t.order_id
--     JOIN Branch_product bp
--         ON bp.branch_product_id = oi.branch_product_id
--     JOIN Product p
--         ON p.product_id = bp.product_id
--     JOIN Category c
--         ON c.category_id = p.category_id
--     WHERE c.category_id <> p_category_id
--     GROUP BY c.category_id, c.name
--     HAVING COUNT(*) >= p_min_count
--     ORDER BY co_occurrence_count DESC;
-- END;
-- $$ LANGUAGE plpgsql;
