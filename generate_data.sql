-- Find the ID of the last order placed to determine the scope of new orders :
SELECT MAX(order_id) FROM ordere;  -- **output => 25260

-- find customres who has layolity:
WITH loyalty AS (
    SELECT
        o.customer_id,
        FLOOR(SUM(oi.quantity * oi.final_price_at_order_time) / 100) AS points_3m
    FROM ordere o
    JOIN order_item oi ON oi.order_id = o.order_id
    WHERE o.order_date >= CURRENT_DATE - INTERVAL '3 months'
    GROUP BY o.customer_id
),
eligible_customers AS (
    SELECT customer_id
    FROM loyalty
    WHERE points_3m > 0
)
SELECT COUNT(*) FROM eligible_customers;


ALTER TABLE public.ordere DISABLE TRIGGER trg_enforce_order_date;

-- Create temp table of random customers whose loyalty points we want to increase:
CREATE TEMP TABLE target_boost AS
SELECT customer_id
FROM customer
WHERE customer_id NOT IN (
    SELECT DISTINCT o.customer_id
    FROM bnpl_plan b
    JOIN ordere o ON o.order_id = b.order_id
)
ORDER BY random()
LIMIT 400;

-- Create Order for selected customers in the last 3 months:
INSERT INTO ordere (customer_id, status, priority, payment_method, order_date)
SELECT
    customer_id,
    'Received',
    'High',
    'In-App Wallet',
    CURRENT_DATE - (random()*45)::int * INTERVAL '1 day'
FROM target_boost,
generate_series(1,3);

ALTER TABLE public.ordere ENABLE TRIGGER trg_enforce_order_date;


INSERT INTO order_item (order_id, branch_product_id, quantity, return_status)
SELECT
    o.order_id,
    bp.branch_product_id,
    (floor(random()*3)+3)::int,
    NULL
FROM ordere o
JOIN target_boost tb ON tb.customer_id = o.customer_id
JOIN LATERAL (
    SELECT branch_product_id, sale_price
    FROM branch_product
    ORDER BY sale_price DESC
    LIMIT 20
) bp ON true
WHERE o.order_date >= CURRENT_DATE - INTERVAL '3 months';

-- Create Orders with BNPL payment method for selected customers:
WITH loyalty AS (
    SELECT
        o.customer_id,
        FLOOR(SUM(oi.quantity * oi.final_price_at_order_time) / 100) AS points_3m
    FROM ordere o
    JOIN order_item oi ON oi.order_id = o.order_id
    WHERE o.order_date >= CURRENT_DATE - INTERVAL '3 months'
    GROUP BY o.customer_id
),
eligible_customers AS (
    SELECT customer_id
    FROM loyalty
    WHERE points_3m > 0
)

INSERT INTO ordere (
    customer_id,
    status,
    priority,
    payment_method,
    order_date
)
SELECT
    customer_id,
    'Received',
    'High',
    'BNPL',
    CURRENT_DATE
FROM eligible_customers;


INSERT INTO order_item (
    order_id,
    branch_product_id,
    quantity,
    final_price_at_order_time,
    return_status
)
SELECT
    o.order_id,
    bp.branch_product_id,
    1,
    bp.price,
    NULL
FROM ordere o
JOIN eligible_customers ec ON ec.customer_id = o.customer_id
JOIN LATERAL (
    SELECT branch_product_id, price
    FROM branch_product
    ORDER BY price ASC
    LIMIT 1
) bp ON true
WHERE o.payment_method = 'BNPL'
AND o.order_date = CURRENT_DATE;

-- Create BNPL plan for Orders with BNPL payment method:
INSERT INTO bnpl_plan (order_id, status)
SELECT order_id, 'Active'
FROM ordere
WHERE payment_method = 'BNPL'
AND order_date = CURRENT_DATE;

-- Create transaction for BNPL repayments:
WITH target_bnpl AS (
    SELECT 
        b.bnpl_id,
        o.order_id,        
        o.customer_id,
        o.order_date,
        SUM(oi.quantity * oi.final_price_at_order_time) AS total_amount,
        FLOOR(random()*4)::int AS paid_installments  
    FROM bnpl_plan b
    JOIN ordere o ON o.order_id = b.order_id
    JOIN order_item oi ON oi.order_id = o.order_id
    WHERE b.status = 'Active'
    GROUP BY b.bnpl_id, o.customer_id, o.order_date, o.order_id
),

installments AS (
    SELECT
        t.bnpl_id,
        t.order_id,           
        w.wallet_id,
        (t.order_date + (n || ' month')::interval)::timestamp AS pay_time,
        ROUND(t.total_amount / 3.0, 2) AS amount
    FROM target_bnpl t
    JOIN wallet w ON w.customer_id = t.customer_id
    JOIN generate_series(1,3) n ON n <= t.paid_installments
),

created_transactions AS (
    INSERT INTO wallet_transaction (
        wallet_id,
        order_id,          
        amount,
        transaction_type,
        transaction_time
    )
    SELECT 
        wallet_id,
        order_id,          
        amount,
        'Payment',
        pay_time
    FROM installments
    RETURNING wallet_id,
              order_id,
              transaction_sequence_number,
              transaction_time,
              amount
)

INSERT INTO repayment (
    bnpl_id,
    wallet_id,
    transaction_sequence_number,
    amount,
    date,
    method
)
SELECT
    i.bnpl_id,
    ct.wallet_id,
    ct.transaction_sequence_number,
    ct.amount,
    ct.transaction_time::date,
    'In-App Wallet'
FROM installments i
JOIN created_transactions ct
  ON ct.wallet_id = i.wallet_id
 AND ct.order_id = i.order_id       
 AND ct.transaction_time = i.pay_time;


-- Create wallet transaction for orders that have In-App Wallet payment method
INSERT INTO wallet_transaction (
    wallet_id,
    order_id,
    amount,
    transaction_type,
    transaction_time
)
SELECT
    w.wallet_id,
    o.order_id,
    SUM(oi.quantity * oi.final_price_at_order_time) AS amount,
    'Payment',
    o.order_date - (floor(random()*2)::int || ' day')::interval
FROM ordere o
JOIN order_item oi ON oi.order_id = o.order_id
JOIN wallet w ON w.customer_id = o.customer_id
WHERE o.payment_method = 'In-App Wallet'
GROUP BY w.wallet_id, o.order_id, o.order_date;


-- Add Withdarawal transactions for wallets that have balance more than 50:
WITH selected_wallets AS (
    SELECT wallet_id, balance
    FROM wallet
    WHERE balance > 50
    ORDER BY random()
    LIMIT (SELECT FLOOR(COUNT(*) * 0.3) FROM wallet)
)
INSERT INTO wallet_transaction (
    wallet_id,
    order_id,        -- NULL
    amount,
    transaction_type,
    transaction_time
)
SELECT
    wallet_id,
    NULL,
    ROUND((balance * (0.1 + random() * 0.3))::numeric, 2) AS amount,
    'Withdrawal',
    CURRENT_DATE - INTERVAL '1 day' * random()
FROM selected_wallets;


-- Add Deposit transactions to match final wallet balances:
WITH wallet_usage AS (
    SELECT
        w.wallet_id,
        w.balance,
        COALESCE(SUM(
            CASE 
                WHEN t.transaction_type IN ('Payment','Withdrawal') THEN t.amount
                ELSE 0
            END
        ),0) AS total_out
    FROM wallet w
    LEFT JOIN Wallet_transaction t
        ON t.wallet_id = w.wallet_id
    GROUP BY w.wallet_id, w.balance
)

INSERT INTO Wallet_transaction (wallet_id, amount, transaction_type, transaction_time)
SELECT
    wu.wallet_id,
    (wu.balance - wu.total_out) AS amount,
    'Deposit' AS transaction_type,
    CURRENT_DATE - INTERVAL '1 day' * random() AS transaction_time
FROM wallet_usage wu
WHERE wu.balance > wu.total_out;  


-- Create return request records :
INSERT INTO return_request (
    order_id,
    branch_product_id,
    reason,
    review_results,
    request_date,
    decision_date
)
SELECT
    oi.order_id,
    oi.branch_product_id,
    (ARRAY['Damaged','Wrong Item','Late Delivery'])[floor(random() * 3 + 1)] AS reason,
    (ARRAY['Return Pending Review','Return Approved','Return Rejected'])[floor(random() * 3 + 1)] AS review_results,
    o.order_date + make_interval(days => (floor(random() * 10) + 1)::int) AS request_date,
    CASE WHEN random() < 0.7
         THEN o.order_date + make_interval(days => (floor(random() * 15) + 11)::int)
         ELSE NULL
    END AS decision_date
FROM (
    -- Pick 500 random order_items that belong to received orders only
    SELECT oi.*
    FROM order_item oi
    JOIN ordere o ON o.order_id = oi.order_id
    WHERE o.status = 'Received'
    ORDER BY random()
    LIMIT 500
) oi
JOIN ordere o ON o.order_id = oi.order_id;



-- Create CSV file of generated data :
-- These CSV files are exported using a graphical client
-- Filters or conditions (e.g., order_id > 25260) are applied 
--    within the export dialog to include only relevant records.