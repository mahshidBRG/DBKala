-- 2- The accounting department needs access to total sales and profits each day.

-- This materialized view provides a daily accounting summary of finalized orders.
-- It aggregates total gross sales, net sales after loyalty discounts,
-- and realized profit using historical cost snapshots.
-- Only completed orders (Shipped, Received) are considered.
-- Loyalty levels are dynamically calculated based on the last 3 months of spending.
-- The view is designed for end-of-day financial reporting and should be refreshed daily.
-- 

CREATE MATERIALIZED VIEW mv_daily_accounting_report AS

-- Select only finalized (realized) orders
    WITH valid_orders AS (
        SELECT order_id, customer_id, order_date
        FROM Ordere
        WHERE status IN ('Shipped', 'Received')
    ),

-- Compute per-order totals (avoid double-counting shipping)
    order_totals AS (
        SELECT
            vo.order_id,
            vo.customer_id,
            vo.order_date,

            -- Total revenue from items (after product-level discount)
            SUM(oi.quantity * oi.final_price_at_order_time) AS items_total,

            -- Shipping cost per order
            COALESCE(s.shipping_cost, 0) AS shipping_cost,

            -- Total historical cost snapshot (for profit calculation)
            SUM(oi.quantity * oi.cost_price_at_order_time) AS total_cost

        FROM valid_orders vo
        JOIN order_item oi 
            ON vo.order_id = oi.order_id
        LEFT JOIN shipment s 
            ON vo.order_id = s.order_id

        GROUP BY vo.order_id, vo.customer_id, vo.order_date, s.shipping_cost
    ),

    -- Calculate loyalty points based on last 3 months of purchases
    loyalty_calc AS (
        SELECT
            vo.customer_id,

            -- 1 point per 100 currency units spent
            FLOOR(SUM(oi.quantity * oi.final_price_at_order_time) / 100) 
                AS loyalty_points

        FROM valid_orders vo
        JOIN order_item oi 
            ON vo.order_id = oi.order_id

        WHERE vo.order_date >= CURRENT_DATE - INTERVAL '3 months'

        GROUP BY vo.customer_id
    ),

    -- Determine loyalty discount percentage
    loyalty_level AS (
        SELECT
            customer_id,

            CASE
                WHEN loyalty_points > 2000 THEN 0.10  -- Gold
                WHEN loyalty_points >= 1000 THEN 0.05 -- Silver
                ELSE 0                                -- Bronze
            END AS loyalty_discount

        FROM loyalty_calc
    )

    -- Final daily aggregation for accounting
    SELECT
        ot.order_date AS report_date,

        -- Gross sales before loyalty discount
        SUM(ot.items_total + ot.shipping_cost) 
            AS total_sales_gross,

        -- Net sales after loyalty discount
        SUM(
            (ot.items_total + ot.shipping_cost)
            * (1 - COALESCE(ll.loyalty_discount, 0))
        ) AS total_sales_after_loyalty,

        -- Real profit (after loyalty discount, using historical cost snapshot)
        SUM(
            (ot.items_total * (1 - COALESCE(ll.loyalty_discount, 0)))
            - ot.total_cost
        ) AS total_profit

FROM order_totals ot
LEFT JOIN loyalty_level ll
    ON ot.customer_id = ll.customer_id
GROUP BY ot.order_date
ORDER BY ot.order_date;

SELECT * FROM mv_daily_accounting_report;
