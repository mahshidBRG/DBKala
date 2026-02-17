-- 10- This function calculates the average user rating for products within a given category and 
--     returns the results sorted in descending order to identify the most popular products.
--     Products are sorted by average rating in descending order,
--     while unrated products appear at the end of the result set.

CREATE OR REPLACE FUNCTION category_product_popularity (
    p_category_name VARCHAR
)
RETURNS TABLE(
    product_id INT,
    product_name VARCHAR,
    average_rating NUMERIC(3,2),
)
AS $$
BEGIN
    RETURN QUERY

    SELECT 
        p.product_id,
        p.name,
        ROUND(AVG(f.rating),2) AS average_rating
    FROM Product p

    JOIN Category c
        ON p.category_id = c.category_id
       AND c.name = p_category_name

    LEFT JOIN Branch_product bp
        ON bp.product_id = p.product_id

    LEFT JOIN order_item oi
        ON oi.branch_id = bp.branch_id
       AND oi.product_id = bp.product_id

    LEFT JOIN feedback f
        ON f.order_id = oi.order_id
       AND f.branch_id = oi.branch_id
       AND f.product_id = oi.product_id

    GROUP BY p.product_id, p.name
    ORDER BY average_rating DESC NULLS LAST;

END;
$$ LANGUAGE plpgsql;