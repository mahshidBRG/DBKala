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
    average_rating NUMERIC(3,2)
)
AS $$
BEGIN
    RETURN QUERY

    SELECT 
        p.product_id,
        p.name,
        ROUND(AVG(f.rating),2) AS average_rating
    FROM Category super_c

    JOIN Category sub_c
        ON sub_c.parent_category_id = super_c.category_id

    JOIN Product p
        ON p.category_id = sub_c.category_id

    LEFT JOIN Branch_product bp
        ON bp.product_id = p.product_id

    LEFT JOIN feedback f
        ON f.branch_product_id = bp.branch_product_id

    WHERE super_c.name = p_category_name

    GROUP BY p.product_id, p.name
    ORDER BY average_rating DESC NULLS LAST;

END;
$$ LANGUAGE plpgsql;


-- Sample input: 'Sports'
SELECT * FROM category_product_popularity('Sports');
