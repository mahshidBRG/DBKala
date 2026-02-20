-- 3- Shows personal information of customers who have 
-- placed at least one order in a specific branch
-- Designed for branch managers to easily see the customers of their branch.
-- Managers can filter the output by branch_id to view only their own branch's customers.

-- Output columns:
-- - Customer personal details: customer_id, name, age, gender, income, email, phone, customer type/status
-- - Branch info: branch_id , branch_name (where the customer made purchase)
CREATE VIEW v_branch_customers AS
SELECT DISTINCT
    c.customer_id,
    c.name AS customer_name,
    c.age,
    c.income,
    c.gender,
    c.email,
    c.phone,
    c.customer_type,
    c.customer_status,
    bp.branch_id,
    b.name AS branch_name
FROM Customer c
JOIN Ordere o ON c.customer_id = o.customer_id
JOIN order_item oi ON o.order_id = oi.order_id
JOIN Branch_product bp ON oi.branch_product_id = bp.branch_product_id
JOIN Branch b ON bp.branch_id = b.branch_id;


SELECT * FROM v_branch_customers;
