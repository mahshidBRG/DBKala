-- 2- This query retrieves products ordered within the specified time range and calculates their average customer rating.
--    The results are sorted in descending order of average rating to highlight the most popular products.

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
    FROM Ordere o
    JOIN order_item oi
        ON o.order_id = oi.order_id
    JOIN feedback f
        ON f.order_id = oi.order_id
       AND f.branch_product_id = oi.branch_product_id
    JOIN Branch_product bp
        ON bp.branch_product_id = oi.branch_product_id
    JOIN Product p
        ON p.product_id = bp.product_id
    WHERE o.order_date BETWEEN p_start AND p_end
    GROUP BY p.product_id, p.name
    ORDER BY AVG(f.rating) DESC;
END;
$$ LANGUAGE plpgsql;


-- Sample input: '2024-08-02' ,'2025-03-31'
SELECT * FROM popular_products_in_range('2024-08-02' ,'2025-03-31');

-- Output :
"product_id"	    "product_name"	        "average_rating"

417	                "LeatherCraft Pants"	    5.00
289	                "SteelChef Blender"	        5.00
337	                "ThinkBook Prime"	        5.00
47	                "GoPro FocusX"	            5.00
619	                "ComfortFit Skirt"	        5.00
538	                "WindGuard Bottle"	        5.00
585	                "UrbanLine Leggings"	    5.00
446	                "Alpine Sweater"	        5.00
266	                "HappyBear Pajamas"	        4.00
282	                "BrightKid Hoodie"	        4.00
110	                "PowerDrive Mat"	        4.00
502	                "OnePlus Prime"	            4.00
309	                "HomeEase Cookware"     	4.00
503	                "OnePlus Nova"	            4.00
416	                "LeatherCraft Shirt"	    3.50
13                  "Ricoh ProShot"	            3.00
45	                "GoPro ProShot"         	3.00
55	                "FujiFilm FocusX"	        3.00
88	                "GreenLeaf Blanket"	        3.00
92	                "CozyNest Diffuser"	        3.00
153	                "RusticPro Shelf"	        3.00
519	                "MotoEdge Pro"	            3.00
562	                "CampPro Tent"	            3.00
600	                "SilkElegance Jacket"	    3.00
447	                "Alpine Shorts"	            2.00
302	                "KitchenPro Cookware"   	2.00
612	                "Floral Skirt"	            2.00
259	                "LittleStar Pajamas"	    2.00
155	                "RusticPro Chair"	        1.00
150	                "RusticPro Table"	        1.00
223	                "HomeGym DartBoard"	        1.00
125	                "GymMaster Kettlebell"	    1.00
556	                "EverTrack Stove"	        1.00
80	                "HomeAura Diffuser"     	1.00
362	                "NitroBlade Infinity"	    1.00
36	                "Lumix Rapid"	            1.00
221	                "HomeGym TennisSet"	        1.00
470	                "ZenPhone Aero"	            1.00
482	                "Redmi S"	                1.00