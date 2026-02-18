-- 1- The discount amount must be between zero and one and the email format must be correct.

-- Discount :
ALTER TABLE Customer
ADD CONSTRAINT check_customer_vat_percent
CHECK (VAT_exemption_percent IS NULL OR VAT_exemption_percent BETWEEN 0 AND 1); 
-- For other discount columns, it is applied in the table definition.

-- Email format :
ALTER TABLE Customer
ADD CONSTRAINT check_email_format
CHECK (
    email IS NULL OR
    email ~ '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$'
);


-- 2- The order registration date must be the same as the time it was registered in the database.

-- In order to prevent past or future orders from being entered,
--  a Trigger was used to ensure that the order_date value was exactly equal to the current date in the database system.
CREATE OR REPLACE FUNCTION enforce_order_date_today()
RETURNS TRIGGER AS $$
BEGIN
    IF NEW.order_date <> CURRENT_DATE THEN
        RAISE EXCEPTION 'Order date must be equal to current date';
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_enforce_order_date
BEFORE INSERT ON Ordere
FOR EACH ROW
EXECUTE FUNCTION enforce_order_date_today();


-- To prevent unauthorized changes to the order date and maintain the accuracy of time information,
--  the ability to update the order_date field has been restricted.
CREATE OR REPLACE FUNCTION prevent_order_date_update()
RETURNS TRIGGER AS $$
BEGIN
    IF NEW.order_date <> OLD.order_date THEN
        RAISE EXCEPTION 'order_date cannot be updated';
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_prevent_order_date_update
BEFORE UPDATE ON Ordere
FOR EACH ROW
EXECUTE FUNCTION prevent_order_date_update();


-- Ensures shipping_date is on or after the related order_date.
-- Prevents shipping before the order is registered.
CREATE OR REPLACE FUNCTION trg_check_shipping_date()
RETURNS TRIGGER AS $$
DECLARE
    ord_date DATE;
BEGIN

    SELECT order_date INTO ord_date
    FROM Ordere
    WHERE order_id = NEW.order_id;

    IF NEW.shipping_date < ord_date THEN
        RAISE EXCEPTION 
        'Shipping date cannot be before order date!';
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER check_shipping_date
BEFORE INSERT OR UPDATE ON shipment
FOR EACH ROW
EXECUTE FUNCTION trg_check_shipping_date();



-- 3 - The condition of the orders must always be clear and consistent with logic.

-- Change allowed values for order_status
ALTER TABLE Ordere 
DROP CONSTRAINT IF EXISTS ordere_status_check;

ALTER TABLE Ordere
ADD CONSTRAINT ordere_status_check 
CHECK (status IN ('Shipped', 'Received', 'Stocking', 'Pending Payment'));

-- -- Trigger ensures 'status' follows the sequence:
-- Stocking → Pending Payment → Shipped → Received
-- and never allows 'Unknown'.
CREATE OR REPLACE FUNCTION trg_validate_order_status()
RETURNS TRIGGER AS $$
DECLARE
    prev_status VARCHAR(30);
BEGIN
    
    IF NEW.status = 'Unknown' THEN
        RAISE EXCEPTION 'Order status cannot be Unknown!';
    END IF;

    SELECT status INTO prev_status
    FROM Ordere
    WHERE order_id = NEW.order_id;

    IF prev_status IS NULL THEN
        IF NEW.status <> 'Stocking' THEN
            RAISE EXCEPTION 'New order must start with Stocking';
        END IF;
    ELSE
        IF prev_status = 'Stocking' AND NEW.status <> 'Pending Payment' THEN
            RAISE EXCEPTION 'Next status after Stocking must be Pending Payment';
        ELSIF prev_status = 'Pending Payment' AND NEW.status <> 'Shipped' THEN
            RAISE EXCEPTION 'Next status after Pending Payment must be Shipped';
        ELSIF prev_status = 'Shipped' AND NEW.status <> 'Received' THEN
            RAISE EXCEPTION 'Next status after Shipped must be Received';
        ELSIF prev_status = 'Received' THEN
            RAISE EXCEPTION 'Order already received; no further updates allowed';
        END IF;
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_order_status
BEFORE INSERT OR UPDATE ON Ordere
FOR EACH ROW
EXECUTE FUNCTION trg_validate_order_status();



-- 4- Order priority may not be Critical for small business customers with low incomes.

-- Trigger ensures Small Business customers with low income
-- cannot have Critical priority on orders.
CREATE OR REPLACE FUNCTION trg_check_small_business_priority()
RETURNS TRIGGER AS $$
DECLARE
    cust_type VARCHAR(50);
    cust_income NUMERIC;
BEGIN
    -- customer informations
    SELECT customer_type INTO cust_type
    FROM Customer
    WHERE customer_id = NEW.customer_id;

    IF cust_type = 'Small Business' AND NEW.priority = 'Critical' THEN  
        RAISE EXCEPTION 'Priority cannot be Critical for Small Business customers with low income!';
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_small_business_priority
BEFORE INSERT OR UPDATE ON Ordere
FOR EACH ROW
EXECUTE FUNCTION trg_check_small_business_priority();


-- 5- It is not possible to send large envelopes by air (post or freight). Boxes should not be sent by ground.

ALTER TABLE shipment
ADD CONSTRAINT chk_shipping_rules
CHECK (
    (
        packaging_type = 'Envelope'
        AND packaging_size = 'Large'
        AND transport_method NOT IN ('Air (Post)', 'Air (Freight)')
    )
    OR
    (
        packaging_type = 'Box'
        AND transport_method <> 'Ground'
    )
);


-- 6- Trigger function: total outstanding BNPL debt must not exceed debt limit (loyalty points * 20)
-- Trigger function:
-- Enforce: a customer's total outstanding BNPL debt must not exceed (loyalty_points_last_3_months * 20)
-- Updates made to match your new BNPL status domain:
--   - BNPL_PLAN.status is ONLY ('Active','Settled')
--   - Therefore we enforce ONLY when NEW.status = 'Active'
-- Fixes applied:
--   1) Removed references to 'Overdue' and 'Canceled'
--   2) Removed undefined variable v_loyalty_discount (it caused runtime errors)
--   3) Outstanding debt is computed as: sum(order_total - paid_amount) over all Active BNPLs (including NEW one)
--   4) Loyalty points are computed from last 3 months purchases: floor(sum(order_total_last_3_months)/100)

CREATE OR REPLACE FUNCTION fn_enforce_customer_debt_limit()
RETURNS trigger AS $$
DECLARE
  v_customer_id       INT;
  v_points_3m         INT;
  v_debt_limit        NUMERIC(14,2);
  v_outstanding_debt  NUMERIC(14,2);
BEGIN
  -- Enforce only when BNPL is Active debt
  IF NEW.status <> 'Active' THEN
    RETURN NEW;
  END IF;

  -- Find customer via the related order
  SELECT o.customer_id
  INTO v_customer_id
  FROM ordere o
  WHERE o.order_id = NEW.order_id;

  IF v_customer_id IS NULL THEN
    RAISE EXCEPTION 'Order % not found or has no customer', NEW.order_id;
  END IF;

  ----------------------------------------------------------------------
  -- Loyalty points in last 3 months:
  -- points = floor( sum(order_total_last_3_months) / 100 )
  -- order_total = sum(quantity * final_price_at_order_time) per order
  ----------------------------------------------------------------------
  SELECT COALESCE(FLOOR(COALESCE(SUM(x.order_amount), 0) / 100), 0)::int
  INTO v_points_3m
  FROM (
    SELECT
      o2.order_id,
      SUM(oi.quantity * oi.final_price_at_order_time) AS order_amount
    FROM ordere o2
    JOIN order_item oi
      ON oi.order_id = o2.order_id
    WHERE o2.customer_id = v_customer_id
      AND o2.order_date >= (CURRENT_DATE - INTERVAL '3 months')
    GROUP BY o2.order_id
  ) x;

  -- Debt limit = points * 20
  v_debt_limit := (v_points_3m * 20)::numeric(14,2);

  ----------------------------------------------------------------------
  -- Outstanding debt across ALL Active BNPLs for this customer (excluding NEW on update):
  -- outstanding per BNPL = GREATEST(order_total - total_repaid, 0)
  ----------------------------------------------------------------------
  SELECT COALESCE(SUM(GREATEST(t.order_total - t.paid_amount, 0)), 0)
  INTO v_outstanding_debt
  FROM (
    SELECT
      bp.bnpl_id,
      base.base_amount::numeric(14,2) AS order_total,
      COALESCE(paid.paid_amount, 0)::numeric(14,2) AS paid_amount
    FROM bnpl_plan bp
    JOIN ordere o3
      ON o3.order_id = bp.order_id
    JOIN (
      SELECT
        oi2.order_id,
        SUM(oi2.quantity * oi2.final_price_at_order_time) AS base_amount
      FROM order_item oi2
      GROUP BY oi2.order_id
    ) base
      ON base.order_id = o3.order_id
    LEFT JOIN (
      SELECT r.bnpl_id, SUM(r.amount) AS paid_amount
      FROM repayment r
      GROUP BY r.bnpl_id
    ) paid
      ON paid.bnpl_id = bp.bnpl_id
    WHERE o3.customer_id = v_customer_id
      AND bp.status = 'Active'
      AND bp.bnpl_id <> COALESCE(NEW.bnpl_id, -1)
  ) t;

  ----------------------------------------------------------------------
  -- Add NEW BNPL's outstanding amount too:
  -- NEW_outstanding = order_total(NEW.order_id) - repayments(NEW.bnpl_id if exists)
  -- On INSERT, NEW.bnpl_id may be NULL, so we treat paid as 0.
  ----------------------------------------------------------------------
  v_outstanding_debt := v_outstanding_debt + COALESCE((
    SELECT SUM(oi3.quantity * oi3.final_price_at_order_time)
    FROM order_item oi3
    WHERE oi3.order_id = NEW.order_id
  ), 0);

  ----------------------------------------------------------------------
  -- Enforce limit
  ----------------------------------------------------------------------
  IF v_outstanding_debt > v_debt_limit THEN
    RAISE EXCEPTION
      'Debt limit exceeded for customer_id=% (outstanding=%.2f, limit=%.2f, points_3m=%)',
      v_customer_id, v_outstanding_debt, v_debt_limit, v_points_3m;
  END IF;

  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Trigger on BNPL_PLAN: enforce on insert and when status/order_id changes
DROP TRIGGER IF EXISTS trg_enforce_customer_debt_limit ON bnpl_plan;

CREATE TRIGGER trg_enforce_customer_debt_limit
BEFORE INSERT OR UPDATE OF status, order_id ON bnpl_plan
FOR EACH ROW
EXECUTE FUNCTION fn_enforce_customer_debt_limit();



-- 7 - a branch must have a manager assigned to it, and a manager can only manage one branch.
-- The first part of the condition is already enforced by the manager_id column in the Branch table being defined as NOT NULL.
-- To enforce the second part, we can add a unique constraint on the manager_id column in the Branch table to ensure that a manager can only be assigned to one branch.
ALTER TABLE Branch
ADD CONSTRAINT uq_branch_manager UNIQUE (manager_id);



-- 8
-- 1. Drop foreign keys that depend on the current composite key
ALTER TABLE public.order_item
  DROP CONSTRAINT fk_order_item_branch_product;
ALTER TABLE public.supply
  DROP CONSTRAINT supply_branch_id_product_id_fkey;

-- 2. Add the surrogate column as SERIAL (creates sequence, fills existing rows)
ALTER TABLE public.branch_product
  ADD COLUMN branch_product_id SERIAL;

-- 3. Drop the old composite primary key
ALTER TABLE public.branch_product
  DROP CONSTRAINT branch_product_pkey;

-- 4. Make the new column the primary key
ALTER TABLE public.branch_product
  ADD PRIMARY KEY (branch_product_id);

-- 5. Re‑establish uniqueness on (branch_id, product_id) for data integrity
ALTER TABLE public.branch_product
  ADD CONSTRAINT branch_product_unique UNIQUE (branch_id, product_id);

-- 6. Make branch_id nullable (now allowed because it's no longer part of PK)
ALTER TABLE public.branch_product
  ALTER COLUMN branch_id DROP NOT NULL;

-- adjust refrences to Branch_product(branch_id,product_id)
-----------------
--supply
-----------------

BEGIN;

ALTER TABLE public.supply
  ADD COLUMN branch_product_id bigint;

UPDATE public.supply s
SET branch_product_id = bp.branch_product_id
FROM public.branch_product bp
WHERE s.branch_id = bp.branch_id
  AND s.product_id = bp.product_id
  AND s.branch_product_id IS NULL;

DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM public.supply WHERE branch_product_id IS NULL) THEN
    RAISE EXCEPTION 'Backfill failed: some supply rows have NULL branch_product_id';
  END IF;
END $$;

ALTER TABLE public.supply
  ALTER COLUMN branch_product_id SET NOT NULL;

ALTER TABLE public.supply
  ADD CONSTRAINT supply_branch_product_id_fkey
  FOREIGN KEY (branch_product_id)
  REFERENCES public.branch_product(branch_product_id)
  ON DELETE CASCADE;

COMMIT;


-----------------
--order_item
-----------------
BEGIN;

ALTER TABLE public.order_item
  ADD COLUMN branch_product_id bigint;

UPDATE public.order_item oi
SET branch_product_id = bp.branch_product_id
FROM public.branch_product bp
WHERE oi.branch_id = bp.branch_id
  AND oi.product_id = bp.product_id
  AND oi.branch_product_id IS NULL;

-- Ensure it filled for all rows
DO $$
BEGIN
  IF EXISTS (
    SELECT 1 FROM public.order_item WHERE branch_product_id IS NULL
  ) THEN
    RAISE EXCEPTION 'Backfill failed: some order_item rows have NULL branch_product_id';
  END IF;
END $$;

ALTER TABLE public.order_item
  ALTER COLUMN branch_product_id SET NOT NULL;

-- New FK to surrogate key
ALTER TABLE public.order_item
  ADD CONSTRAINT order_item_branch_product_id_fkey
  FOREIGN KEY (branch_product_id)
  REFERENCES public.branch_product(branch_product_id)
  ON DELETE RESTRICT;

COMMIT;

--for references to order_item
ALTER TABLE public.order_item
  ADD CONSTRAINT order_item_order_id_branch_product_id_uk
  UNIQUE (order_id, branch_product_id);
-----------------
--feedback
-----------------

BEGIN;

ALTER TABLE public.feedback
  ADD COLUMN branch_product_id bigint;

UPDATE public.feedback f
SET branch_product_id = oi.branch_product_id
FROM public.order_item oi
WHERE f.order_id = oi.order_id
  AND f.branch_id = oi.branch_id
  AND f.product_id = oi.product_id
  AND f.branch_product_id IS NULL;

DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM public.feedback WHERE branch_product_id IS NULL) THEN
    RAISE EXCEPTION 'Backfill failed: some feedback rows have NULL branch_product_id';
  END IF;
END $$;

ALTER TABLE public.feedback
  ALTER COLUMN branch_product_id SET NOT NULL;

ALTER TABLE public.feedback
  ADD CONSTRAINT feedback_order_item_surrogate_fkey
  FOREIGN KEY (order_id, branch_product_id)
  REFERENCES public.order_item(order_id, branch_product_id)
  ON DELETE CASCADE;

COMMIT;

-- Drop old FK (name from your dump)
ALTER TABLE public.feedback
  DROP CONSTRAINT fk_feedback_order_item;


--return_request
-----------------
BEGIN;

ALTER TABLE public.return_request
  ADD COLUMN branch_product_id bigint;

UPDATE public.return_request rr
SET branch_product_id = oi.branch_product_id
FROM public.order_item oi
WHERE rr.order_id = oi.order_id
  AND rr.branch_id = oi.branch_id
  AND rr.product_id = oi.product_id
  AND rr.branch_product_id IS NULL;

DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM public.return_request WHERE branch_product_id IS NULL) THEN
    RAISE EXCEPTION 'Backfill failed: some return_request rows have NULL branch_product_id';
  END IF;
END $$;

ALTER TABLE public.return_request
  ALTER COLUMN branch_product_id SET NOT NULL;

ALTER TABLE public.return_request
  ADD CONSTRAINT return_request_order_item_surrogate_fkey
  FOREIGN KEY (order_id, branch_product_id)
  REFERENCES public.order_item(order_id, branch_product_id)
  ON DELETE CASCADE;

COMMIT;

-- Drop old FK (name from your dump)
ALTER TABLE public.return_request
  DROP CONSTRAINT fk_return_request_order_item;
----update pks for feedback and return_request
ALTER TABLE public.feedback
  DROP CONSTRAINT feedback_pkey;

ALTER TABLE public.feedback
  ADD CONSTRAINT feedback_pkey PRIMARY KEY (order_id, branch_product_id);

ALTER TABLE public.return_request
  DROP CONSTRAINT return_request_pkey;

ALTER TABLE public.return_request
  ADD CONSTRAINT return_request_pkey PRIMARY KEY (order_id, branch_product_id);
-----clean up
BEGIN;

-- FEEDBACK: change PK from (order_id, branch_id, product_id) to (order_id, branch_product_id)
ALTER TABLE public.feedback
  DROP CONSTRAINT IF EXISTS feedback_pkey;

ALTER TABLE public.feedback
  ADD CONSTRAINT feedback_pkey PRIMARY KEY (order_id, branch_product_id);

-- RETURN_REQUEST: change PK similarly
ALTER TABLE public.return_request
  DROP CONSTRAINT IF EXISTS return_request_pkey;

ALTER TABLE public.return_request
  ADD CONSTRAINT return_request_pkey PRIMARY KEY (order_id, branch_product_id);

COMMIT;

------------------------------------------------------------------------
ALTER TABLE order_item DROP COLUMN branch_id, DROP COLUMN product_id;
ALTER TABLE supply DROP COLUMN branch_id, DROP COLUMN product_id;
ALTER TABLE feedback DROP COLUMN branch_id, DROP COLUMN product_id;
ALTER TABLE return_request DROP COLUMN branch_id, DROP COLUMN product_id;
------------------------------------------------------------------------
-- Trigger function: when a branch is deleted, anonymize customers who ONLY ordered from that branch
-- while keeping all order/shipment history intact (no order rows are deleted).
-- This must run BEFORE DELETE because Branch_product.branch_id will become NULL due to ON DELETE SET NULL,
-- and after that we cannot reliably identify which branch_products belonged to the deleted branch.

CREATE OR REPLACE FUNCTION fn_branch_delete_anonymize_customers()
RETURNS trigger AS $$
BEGIN
  /*
    Rule:
    - Keep order history (order_item, shipment, products, etc.) after deleting a branch.
    - Remove personal info (PII) for customers who have NO orders in any other branch.

    After your migration:
    - order_item references branch_product via branch_product_id
    - branch_product.branch_id becomes NULL after branch deletion (ON DELETE SET NULL)
    Therefore we detect affected customers using OLD.branch_id before the delete happens.
  */

  UPDATE customer c
  SET
    name   = 'Deleted Customer',
    age    = NULL,
    income = NULL,
    gender = NULL,
    email  = NULL,
    phone  = NULL
  WHERE c.customer_id IN (

    -- Customers who have at least one order item from the branch being deleted
    SELECT o.customer_id
    FROM ordere o
    WHERE EXISTS (
      SELECT 1
      FROM order_item oi
      JOIN branch_product bp
        ON bp.branch_product_id = oi.branch_product_id
      WHERE oi.order_id = o.order_id
        AND bp.branch_id = OLD.branch_id
    )

    EXCEPT

    -- Exclude customers who also ordered from any other (remaining) branch
    SELECT o2.customer_id
    FROM ordere o2
    WHERE EXISTS (
      SELECT 1
      FROM order_item oi2
      JOIN branch_product bp2
        ON bp2.branch_product_id = oi2.branch_product_id
      WHERE oi2.order_id = o2.order_id
        AND bp2.branch_id IS NOT NULL
        AND bp2.branch_id <> OLD.branch_id
    )
  );

  RETURN OLD;
END;
$$ LANGUAGE plpgsql;


-- Trigger: executes the anonymization logic before deleting a branch row
DROP TRIGGER IF EXISTS trg_branch_delete_anonymize_customers ON branch;

CREATE TRIGGER trg_branch_delete_anonymize_customers
BEFORE DELETE ON branch
FOR EACH ROW
EXECUTE FUNCTION fn_branch_delete_anonymize_customers();

-- 9
ALTER TABLE order_item
ALTER COLUMN return_status SET DEFAULT 'Return Pending Review';

UPDATE order_item
SET return_status = 'Return Pending Review'
WHERE return_status IS NULL;

ALTER TABLE order_item
ALTER COLUMN return_status SET NOT NULL;
CREATE OR REPLACE FUNCTION fn_enforce_return_status_flow()
RETURNS trigger AS $$
BEGIN
  
  IF NEW.return_status IS NOT DISTINCT FROM OLD.return_status THEN
    RETURN NEW;
  END IF;

  IF NEW.return_status NOT IN ('Return Pending Review', 'Return Approved', 'Return Rejected') THEN
    RAISE EXCEPTION 'Invalid return_status: %', NEW.return_status;
  END IF;


  IF OLD.return_status = 'Return Pending Review' THEN
    IF NEW.return_status IN ('Return Approved', 'Return Rejected', 'Return Pending Review') THEN
      RETURN NEW;
    ELSE
      RAISE EXCEPTION 'Invalid transition from % to %', OLD.return_status, NEW.return_status;
    END IF;
  ELSE
   
    RAISE EXCEPTION 'Return status is final; cannot transition from % to %', OLD.return_status, NEW.return_status;
  END IF;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_return_status_flow ON order_item;

CREATE TRIGGER trg_return_status_flow
BEFORE UPDATE OF return_status ON order_item
FOR EACH ROW
EXECUTE FUNCTION fn_enforce_return_status_flow();

-- 10 - The feedback's length should be less than 800 chars the other constraint has been implemented in phase 1
ALTER TABLE feedback
ADD CONSTRAINT chk_feedback_comment_len
CHECK (comment_text IS NULL OR char_length(comment_text) <= 800);


-- Additional compatibility conditions‌:

-- 1- Automatic Order Refund Rule upon Approved Return

-- If the new value becomes 'Approved' and was not previously approved:
-- - The refund amount is calculated as:
-- -- quantity × final_price_at_order_time

-- - The payment method of the order is checked.
-- - If the payment method is not BNPL:
-- -- The refund amount is credited to the customer's wallet.
-- -- A 'Deposit' record is inserted into wallet_transaction.

-- If the payment method is BNPL:
-- - No wallet refund is issued, since the customer has not yet paid the full amount. The outstanding balance is handled dynamically.

'Return Pending Review', 'Return Approved', 'Return Rejected'
'Unknown', 'Shipped', 'Received', 'Stocking', 'Pending Payment'

CCREATE OR REPLACE FUNCTION trg_refund_after_return()
RETURNS TRIGGER AS $$
DECLARE
    refund_amount NUMERIC;
    cust_id INT;
    pay_method VARCHAR(20);
    w_id INT;
BEGIN

    IF NEW.review_results = 'Return Approved'
       AND (OLD.review_results IS DISTINCT FROM 'Return Approved') THEN 

        SELECT quantity * final_price_at_order_time
        INTO refund_amount
        FROM order_item
        WHERE order_id = NEW.order_id
          AND branch_id = NEW.branch_id
          AND product_id = NEW.product_id;

        SELECT o.customer_id, o.payment_method
        INTO cust_id, pay_method
        FROM Ordere o
        WHERE o.order_id = NEW.order_id;

        IF pay_method <> 'BNPL' THEN

            SELECT wallet_id INTO w_id
            FROM Wallet
            WHERE customer_id = cust_id;

            INSERT INTO Wallet_transaction(
                wallet_id,
                amount,
                transaction_type
            )
            VALUES (
                w_id,
                refund_amount,
                'Deposit'
            );

        END IF;

    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER refund_after_return
AFTER UPDATE OF review_results
ON return_request
FOR EACH ROW
EXECUTE FUNCTION trg_refund_after_return();



-- 2- Wallet balance must always reflect the net effect of wallet transactions.
-- If the transaction is deposit, it will be added to the balance.
-- If the transaction is payment or withdrawal, it will be deducted from the balance.
CREATE OR REPLACE FUNCTION trg_wallet_balance_sync()
RETURNS TRIGGER AS $$
BEGIN

    IF NEW.transaction_type = 'Deposit' THEN
        UPDATE Wallet
        SET balance = balance + NEW.amount
        WHERE wallet_id = NEW.wallet_id;

    ELSIF NEW.transaction_type IN ('Payment','Withdrawal') THEN
        UPDATE Wallet
        SET balance = balance - NEW.amount
        WHERE wallet_id = NEW.wallet_id;

    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER wallet_balance_after_insert
AFTER INSERT
ON Wallet_transaction
FOR EACH ROW
EXECUTE FUNCTION trg_wallet_balance_sync();

-- Wallet transactions are defined as immutable to preserve financial integrity and auditability.
CREATE OR REPLACE FUNCTION trg_prevent_wallet_tx_modification()
RETURNS TRIGGER AS $$
BEGIN
    RAISE EXCEPTION 
    'Wallet transactions are immutable and cannot be modified or deleted.';
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER prevent_wallet_tx_modification
BEFORE UPDATE OR DELETE
ON wallet_transaction
FOR EACH ROW
EXECUTE FUNCTION trg_prevent_wallet_tx_modification();

-- 4-Trigger function: set BNPL_PLAN.status to 'Settled' when total repayments >= total order amount
-- Assumptions:
-- 1) A BNPL belongs to exactly one order (bnpl_plan.order_id is UNIQUE).
-- 2) Total due for that BNPL = SUM(quantity * final_price_at_order_time) for the related order.
-- 3) Total paid = SUM(repayment.amount) for that bnpl_id.
-- 4) BNPL status domain is only ('Active','Settled') (per your new CHECK constraint).
-- Notes:
-- - This runs AFTER changes to repayment, because repayments are what make a BNPL "paid".
-- - It only flips to Settled (it does not flip back to Active if you later delete repayments).

-- Drop the old CHECK constraint
ALTER TABLE bnpl_plan
DROP CONSTRAINT IF EXISTS bnpl_plan_status_check;

-- Add the new allowed-status CHECK
ALTER TABLE bnpl_plan
ADD CONSTRAINT bnpl_plan_status_check
CHECK (status IN ('Active', 'Settled'));

CREATE OR REPLACE FUNCTION fn_set_bnpl_settled_when_paid()
RETURNS trigger AS $$
DECLARE
  v_bnpl_id     INT;
  v_order_id    INT;
  v_total_due   NUMERIC(14,2);
  v_total_paid  NUMERIC(14,2);
BEGIN
  -- Identify affected BNPL id (works for INSERT/UPDATE/DELETE on repayment)
  v_bnpl_id := COALESCE(NEW.bnpl_id, OLD.bnpl_id);

  -- Get the related order
  SELECT bp.order_id
  INTO v_order_id
  FROM bnpl_plan bp
  WHERE bp.bnpl_id = v_bnpl_id;

  IF v_order_id IS NULL THEN
    RETURN NULL;
  END IF;

  -- Compute total due from order items
  SELECT COALESCE(SUM(oi.quantity * oi.final_price_at_order_time), 0)
  INTO v_total_due
  FROM order_item oi
  WHERE oi.order_id = v_order_id;

  -- Compute total paid from repayments
  SELECT COALESCE(SUM(r.amount), 0)
  INTO v_total_paid
  FROM repayment r
  WHERE r.bnpl_id = v_bnpl_id;

  -- If fully paid, mark as Settled
  IF v_total_due > 0 AND v_total_paid >= v_total_due THEN
    UPDATE bnpl_plan
    SET status = 'Settled'
    WHERE bnpl_id = v_bnpl_id
      AND status <> 'Settled';
  END IF;

  RETURN NULL;
END;
$$ LANGUAGE plpgsql;

-- Triggers on repayment: any payment insertion/update/deletion may complete the BNPL
DROP TRIGGER IF EXISTS trg_bnpl_settled_after_repayment_ins ON repayment;
DROP TRIGGER IF EXISTS trg_bnpl_settled_after_repayment_upd ON repayment;
DROP TRIGGER IF EXISTS trg_bnpl_settled_after_repayment_del ON repayment;

CREATE TRIGGER trg_bnpl_settled_after_repayment_ins
AFTER INSERT ON repayment
FOR EACH ROW
EXECUTE FUNCTION fn_set_bnpl_settled_when_paid();

CREATE TRIGGER trg_bnpl_settled_after_repayment_upd
AFTER UPDATE OF amount, bnpl_id ON repayment
FOR EACH ROW
EXECUTE FUNCTION fn_set_bnpl_settled_when_paid();

CREATE TRIGGER trg_bnpl_settled_after_repayment_del
AFTER DELETE ON repayment
FOR EACH ROW
EXECUTE FUNCTION fn_set_bnpl_settled_when_paid();

-- 5-- Compute final_price_at_order_time on INSERT if not provided
-- final_price_at_order_time = unit_price_after_discounts
-- unit_price_after_discounts = sale_price * (1 - branch_product.discount)
-- Note: This computes per-unit final price OR total? (Your column name suggests per-item total.)
-- Here we store the TOTAL line amount: quantity * unit_price_after_discounts.

CREATE OR REPLACE FUNCTION fn_set_final_price_at_order_time()
RETURNS trigger AS $$
DECLARE
  v_sale_price NUMERIC(12,2);
  v_discount   NUMERIC(3,2);
BEGIN
  -- Get current branch_product price/discount
  SELECT bp.sale_price, COALESCE(bp.discount, 0)
    INTO v_sale_price, v_discount
  FROM branch_product bp
  WHERE bp.branch_product_id = NEW.branch_product_id;

  IF v_sale_price IS NULL THEN
    RAISE EXCEPTION 'Invalid branch_product_id: %', NEW.branch_product_id;
  END IF;

  -- If not provided by application, compute it
  IF NEW.final_price_at_order_time IS NULL THEN
    NEW.final_price_at_order_time :=
      (v_sale_price * (1 - v_discount))::numeric(12,2);
  END IF;

  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_set_final_price_at_order_time ON order_item;

CREATE TRIGGER trg_set_final_price_at_order_time
BEFORE INSERT ON order_item
FOR EACH ROW
EXECUTE FUNCTION fn_set_final_price_at_order_time();


-- 6- Prevent changing final_price_at_order_time after insert (keeps history consistent)
CREATE OR REPLACE FUNCTION fn_prevent_final_price_update()
RETURNS trigger AS $$
BEGIN
  IF NEW.final_price_at_order_time IS DISTINCT FROM OLD.final_price_at_order_time THEN
    RAISE EXCEPTION 'final_price_at_order_time cannot be updated (historical snapshot)';
  END IF;
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_prevent_final_price_update ON order_item;

CREATE TRIGGER trg_prevent_final_price_update
BEFORE UPDATE OF final_price_at_order_time ON order_item
FOR EACH ROW
EXECUTE FUNCTION fn_prevent_final_price_update();
