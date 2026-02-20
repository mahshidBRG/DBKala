-- 1- Shows number of unshipped orders per product
-- without displaying customer information

-- Output columns:
-- - Product info: product_id, name
-- - Branch info: branch_id, name
-- - pending_orders_count: Number of pending orders
-- - total_quantity_pending: Total quantity requested
CREATE VIEW v_pending_orders_by_product AS
SELECT 
    p.product_id,
    p.name AS product_name,
    bp.branch_id,
    b.name AS branch_name,
    COUNT(DISTINCT o.order_id) AS pending_orders_count,
    SUM(oi.quantity) AS total_quantity_pending
FROM Product p
JOIN Branch_product bp ON p.product_id = bp.product_id
JOIN Branch b ON bp.branch_id = b.branch_id
LEFT JOIN order_item oi ON bp.branch_product_id = oi.branch_product_id
LEFT JOIN Ordere o ON oi.order_id = o.order_id 
    AND o.status NOT IN ('Shipped', 'Received')
GROUP BY p.product_id, p.name, bp.branch_id, b.name
HAVING COUNT(DISTINCT o.order_id) > 0;

SELECT * FROM v_pending_orders_by_product;
