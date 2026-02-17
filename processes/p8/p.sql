-- 8- In this operation, the annual total of deposits and withdrawals is calculated per customer.
--    Then, the average wallet turnover is computed per year, gender, and income group.

-- Although the title refers to “Wallet Adoption Rate”,
--  the required output as specified in the project description corresponds to the average annual wallet turnover.
--  Therefore, the implementation follows the detailed specification.

WITH yearly_customer_turnover AS 
(
    SELECT 
        w.customer_id, 
        EXTRACT(YEAR FROM wt.transaction_time) AS t_year,
        SUM(wt.amount) AS total_turnover
    FROM Wallet_transaction wt
    JOIN Wallet w 
        ON w.wallet_id = wt.wallet_id
    WHERE wt.transaction_type IN ('Deposit', 'Withdrawal') 
    GROUP BY w.customer_id, t_year
),

years AS (
    SELECT DISTINCT t_year FROM yearly_customer_turnover
)

SELECT 
    y.t_year AS year,
    c.gender,

    CASE
        WHEN c.income < 50000 THEN '0-49999'
        WHEN c.income < 100000 THEN '50000-99999'
        ELSE '100000+'
    END AS income_group,

    AVG(COALESCE(yct.total_turnover, 0)) AS average_wallet_turnover

FROM years y
CROSS JOIN Customer c

LEFT JOIN yearly_customer_turnover yct
    ON yct.customer_id = c.customer_id
   AND yct.t_year = y.t_year

GROUP BY 
    y.t_year,
    c.gender,
    CASE
        WHEN c.income < 50000 THEN '0-49999'
        WHEN c.income < 100000 THEN '50000-99999'
        ELSE '100000+'
    END

ORDER BY y.t_year, c.gender;