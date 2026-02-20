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


-- Sample input: 'Laptops', 3500
SELECT * FROM category_association_by_name('Laptops', 3500);

-- Output:
"related_category_id"   	"related_category_name" 	"co_occurrence_count"

21	                                "Cameras"	                3689
32	                                "Women"	                    3635
22	                                "Decor"	                    3620
25	                                "Indoor"                	3585
24	                                "Furniture"                 3569
23	                                "Fitness"	                3560
31	                                "Outdoor"	                3546
27	                                "Kitchen"	                3533
26	                                "Kids"	                    3517
