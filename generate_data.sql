-- find customers who can use and pay BNPL :
WITH last_3m_total AS (
  SELECT
    c.customer_id,
    COALESCE(
      SUM(oi.quantity * oi.final_price_at_order_time),
      0
    ) AS total_last_3m
  FROM customer c
  LEFT JOIN ordere o
    ON o.customer_id = c.customer_id
   AND o.status = 'Received'
   AND o.order_date >= CURRENT_DATE - INTERVAL '3 months'
  LEFT JOIN order_item oi
    ON oi.order_id = o.order_id
  GROUP BY c.customer_id
),

loyalty AS (
  SELECT
    customer_id,
    FLOOR(total_last_3m / 100) AS loyalty_score
  FROM last_3m_total
),

current_debt AS (
  SELECT
    o.customer_id,
    COALESCE(SUM(
      CASE
        WHEN b.status IN ('Active','Overdue')
        THEN o.total_amount
        ELSE 0
      END
    ),0) AS unpaid_debt
  FROM ordere o
  LEFT JOIN bnpl_plan b
    ON b.order_id = o.order_id
  GROUP BY o.customer_id
),

eligible_customers AS (
    SELECT
        l.customer_id
    FROM loyalty l
    LEFT JOIN current_debt d
        ON d.customer_id = l.customer_id
    WHERE l.loyalty_score > 0
      AND l.credit_limit > COALESCE(d.unpaid_debt, 0)
)

-- Select 50 customers from eligible customers and insert order by BNPL payment for them:
INSERT INTO ordere (customer_id, status, priority, payment_method)
SELECT
    ec.customer_id,
    'Received',
    'High',
    'BNPL'
FROM eligible_customers ec
ORDER BY random()
LIMIT 100;

-- Insert multiple unique order items per order :
INSERT INTO order_item (order_id, branch_product_id, quantity, price)
SELECT
    o.order_id,
    bp.branch_product_id,
    -- random quantity between 1 and 5
    (floor(random() * 5) + 1)::int,
    bp.price
FROM ordere o
-- generate 1-5 items per order
JOIN LATERAL (
    SELECT bp2.branch_product_id, bp2.price
    FROM branch_product bp2
    WHERE bp2.branch_id = (
        -- pick a random branch for this order
        SELECT branch_id
        FROM branch_product
        ORDER BY random()
        LIMIT 1
    )
    ORDER BY random()
    LIMIT (floor(random() * 5) + 1)::int  -- number of items
) bp ON true
WHERE o.payment_method = 'BNPL';

-- create bnpl_plan for orders that paied by BNPL:
INSERT INTO bnpl_plan (order_id, status)
SELECT order_id, 'Active'
FROM ordere
WHERE payment_method = 'BNPL';


-- Create Payment transactions for normal Wallet orders (exclude BNPL)
INSERT INTO Wallet_transaction (wallet_id, amount, transaction_type, transaction_time)
SELECT
    w.wallet_id,
    oi.price * oi.quantity AS amount,
    'Payment' AS transaction_type,
    o.order_date - INTERVAL '1 day' * random() AS transaction_time
FROM ordere o
JOIN order_item oi ON oi.order_id = o.order_id
JOIN wallet w ON w.customer_id = o.customer_id
WHERE o.payment_method = 'In-App Wallet'
  AND NOT EXISTS (
      SELECT 1
      FROM bnpl_plan b
      WHERE b.order_id = o.order_id
  );

-- Create Payment transactions for active BNPL plans
WITH target_bnpl AS (
    SELECT 
        b.bnpl_id,
        o.customer_id,
        o.total_amount,
        o.order_date
    FROM bnpl_plan b
    JOIN ordere o ON o.order_id = b.order_id
    WHERE b.status = 'Active'
    ORDER BY random()
    LIMIT 60
),
installments AS (
    SELECT
        t.bnpl_id,
        w.wallet_id,
        (t.order_date + (n || ' month')::interval)::timestamp AS pay_time,
        ROUND(t.total_amount / 3.0, 2) AS amount
    FROM target_bnpl t
    JOIN wallet w ON w.customer_id = t.customer_id,
         generate_series(1,3) n
),
created_transactions AS (
    INSERT INTO Wallet_transaction (wallet_id, amount, transaction_type, transaction_time)
    SELECT
        wallet_id,
        amount,
        'Payment',
        pay_time
    FROM installments
    RETURNING wallet_id, transaction_sequence_number, amount, transaction_time
)

-- Register BNPL repayments in repayment table for repayments that have In-App Wallet methods
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
FROM created_transactions ct
JOIN installments i
  ON i.wallet_id = ct.wallet_id
 AND i.pay_time = ct.transaction_time
 AND i.amount = ct.amount;

-- Register BNPL repayments in repayment table for repayments that have a method other than In-App Wallet
 WITH bnpl_without_repayment AS (
    SELECT b.bnpl_id, o.customer_id, o.total_amount, o.order_date
    FROM bnpl_plan b
    JOIN ordere o ON o.order_id = b.order_id
    LEFT JOIN repayment r ON r.bnpl_id = b.bnpl_id
    WHERE r.bnpl_id IS NULL
      AND b.status = 'Active'
)
INSERT INTO repayment (bnpl_id, wallet_id, amount, date, method)
SELECT
    b.bnpl_id,
    NULL,
    ROUND(b.total_amount / 3.0, 2) AS amount,
    (b.order_date + (n || ' month')::interval)::date AS date,
    (ARRAY['Cash','Credit Card','Debit Card'])[floor(random() * 3 + 1)] AS method
FROM bnpl_without_repayment b,
     generate_series(1, (floor(random() * 4))::int) n;  

-- Add Deposit transactions to match final wallet balances
WITH wallet_totals AS (
    SELECT
        w.wallet_id,
        w.balance,
        COALESCE(SUM(t.amount),0) AS total_payments
    FROM wallet w
    LEFT JOIN Wallet_transaction t 
        ON t.wallet_id = w.wallet_id
        AND t.transaction_type = 'Payment'
    GROUP BY w.wallet_id, w.balance
)
INSERT INTO Wallet_transaction (wallet_id, amount, transaction_type, transaction_time)
SELECT
    wt.wallet_id,
    (wt.balance - wt.total_payments) AS amount,
    'Deposit' AS transaction_type,
    CURRENT_DATE - INTERVAL '1 day' * random() AS transaction_time
FROM wallet_totals wt
WHERE wt.balance > wt.total_payments;


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
    oi.order_date + (floor(random() * 10) + 1)::int || ' days'::interval AS request_date,
    CASE WHEN random() < 0.7
         THEN oi.order_date + (floor(random() * 15) + 11)::int || ' days'::interval
         ELSE NULL
    END AS decision_date
FROM (
    -- Pick 100 random order_items
    SELECT *
    FROM order_item
    ORDER BY random()
    LIMIT 100
) oi;