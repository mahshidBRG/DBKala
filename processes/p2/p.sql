-- 2- This query retrieves products ordered within the specified time range and calculates their average customer rating.
--  The results are sorted in descending order of average rating to highlight the most popular products.

CREATE OR REPLACE FUNCTION popular_products_in_range(
    p_start DATE,
    p_end DATE
)
RETURNS TABLE (
    product_id INT,
    product_name VARCHAR,
    average_rating NUMERIC
)
AS $$
BEGIN
    RETURN QUERY
    SELECT 
        p.product_id,
        p.name,
        AVG(f.rating)
    FROM ordere o
    JOIN order_item oi
        ON o.order_id = oi.order_id
    JOIN feedback f
        ON f.order_id = oi.order_id
       AND f.branch_product_id = oi.branch_product_id
    JOIN branch_product bp
        ON bp.branch_product_id = oi.branch_product_id
    JOIN product p
        ON p.product_id = bp.product_id
    WHERE o.order_date BETWEEN p_start AND p_end
    GROUP BY p.product_id, p.name
    ORDER BY AVG(f.rating) DESC;
END;
$$ LANGUAGE plpgsql;