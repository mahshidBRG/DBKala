-- 12-  Customer Real Value :

-- Real value is defined as the total amount paid for finalized orders
-- (including regular payments and BNPL repayments),
-- minus the amount of approved returned items,
-- plus the total VAT paid.

-- To prevent double-counting, each component
-- (non-BNPL orders, BNPL repayments, returns, and VAT)
-- is calculated separately and then aggregated at the customer level.

WITH finalized_orders AS (
    SELECT order_id, customer_id
    FROM Ordere
    WHERE status IN ('Shipped','Received')
),

non_bnpl_totals AS (
    SELECT 
        fo.customer_id,
        SUM(oi.quantity * oi.final_price_at_order_time) AS total_non_bnpl
    FROM finalized_orders fo
    JOIN Ordere o ON fo.order_id = o.order_id
    LEFT JOIN BNPL_PLAN b ON o.order_id = b.order_id
    JOIN order_item oi ON o.order_id = oi.order_id
    WHERE b.bnpl_id IS NULL
    GROUP BY fo.customer_id
),

bnpl_repayments AS (
    SELECT 
        o.customer_id,
        SUM(r.amount) AS total_bnpl_paid
    FROM repayment r
    JOIN BNPL_PLAN b ON r.bnpl_id = b.bnpl_id
    JOIN Ordere o ON b.order_id = o.order_id
    WHERE o.status IN ('Shipped','Received')
    GROUP BY o.customer_id
),

approved_returns AS (
    SELECT 
        o.customer_id,
        SUM(oi.quantity * oi.final_price_at_order_time) AS total_returns
    FROM Ordere o
    JOIN order_item oi ON o.order_id = oi.order_id
    WHERE o.status IN ('Shipped','Received')
      AND oi.return_status = 'Return Approved'
    GROUP BY o.customer_id
),

vat_paid AS (
    SELECT 
        o.customer_id,
        SUM(
            oi.quantity * oi.final_price_at_order_time
            * 0.10
            * (1 - COALESCE(p.VAT_exemption_percent,0))
        ) AS total_vat
    FROM Ordere o
    JOIN order_item oi ON o.order_id = oi.order_id
    JOIN Branch_product bp ON oi.branch_product_id = bp.branch_product_id
    JOIN Product p ON bp.product_id = p.product_id
    WHERE o.status IN ('Shipped','Received')
    GROUP BY o.customer_id
)

SELECT 
    c.customer_id,
    c.name,

    COALESCE(nb.total_non_bnpl,0)
    + COALESCE(br.total_bnpl_paid,0)
    - COALESCE(ar.total_returns,0)
    + COALESCE(v.total_vat,0)

    AS real_value

FROM customer c
LEFT JOIN non_bnpl_totals nb ON c.customer_id = nb.customer_id
LEFT JOIN bnpl_repayments br ON c.customer_id = br.customer_id
LEFT JOIN approved_returns ar ON c.customer_id = ar.customer_id
LEFT JOIN vat_paid v ON c.customer_id = v.customer_id;