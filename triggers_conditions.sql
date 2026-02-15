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
CREATE OR REPLACE FUNCTION fn_enforce_customer_debt_limit()
RETURNS trigger AS $$
DECLARE
  v_customer_id       INT;
  v_points_3m         INT;
  v_debt_limit        NUMERIC(14,2);
  v_outstanding_debt  NUMERIC(14,2);
BEGIN
  -- We only enforce when this BNPL counts as "debt"
  IF NEW.status NOT IN ('Active', 'Overdue') THEN
    RETURN NEW;
  END IF;

  -- Find customer of this BNPL via the order
  SELECT o.customer_id
  INTO v_customer_id
  FROM ordere o
  WHERE o.order_id = NEW.order_id;

  IF v_customer_id IS NULL THEN
    RAISE EXCEPTION 'Order % not found or has no customer', NEW.order_id;
  END IF;

  ----------------------------------------------------------------------
  -- 1) Compute loyalty points in last 3 months: floor(sum(order_amount)/100)
  -- order_amount = sum(quantity * final_price_at_order_time) per order
  ----------------------------------------------------------------------
  SELECT COALESCE(FLOOR(COALESCE(SUM(x.order_amount),0) / 100), 0)::int
  INTO v_points_3m
  FROM (
    SELECT
      o2.order_id,
      SUM(oi.quantity * oi.final_price_at_order_time) AS order_amount
    FROM ordere o2
    JOIN order_item oi ON oi.order_id = o2.order_id
    WHERE o2.customer_id = v_customer_id
      AND o2.order_date >= (CURRENT_DATE - INTERVAL '3 months')
    GROUP BY o2.order_id
  ) x;


  ----------------------------------------------------------------------
  -- Debt limit = points * 20
  ----------------------------------------------------------------------
  v_debt_limit := (v_points_3m * 20)::numeric(14,2);

  ----------------------------------------------------------------------
  -- Compute customer's total outstanding debt over Active/Overdue BNPLs
  -- outstanding per BNPL = (order_amount_after_loyalty - sum(repayment))
  -- order_amount_after_loyalty = base_amount * (1 - v_loyalty_discount)
  --
  -- Exclude current bnpl_id on UPDATE to avoid double-counting.
  ----------------------------------------------------------------------
  SELECT COALESCE(SUM(GREATEST(t.after_loyalty - t.paid_amount, 0)), 0)
  INTO v_outstanding_debt
  FROM (
    SELECT
      bp.bnpl_id,
      (base.base_amount * (1 - v_loyalty_discount))::numeric(14,2) AS after_loyalty,
      COALESCE(paid.paid_amount, 0)::numeric(14,2) AS paid_amount
    FROM bnpl_plan bp
    JOIN ordere o3 ON o3.order_id = bp.order_id
    JOIN (
      SELECT
        oi2.order_id,
        SUM(oi2.quantity * oi2.final_price_at_order_time) AS base_amount
      FROM order_item oi2
      GROUP BY oi2.order_id
    ) base ON base.order_id = o3.order_id
    LEFT JOIN (
      SELECT r.bnpl_id, SUM(r.amount) AS paid_amount
      FROM repayment r
      GROUP BY r.bnpl_id
    ) paid ON paid.bnpl_id = bp.bnpl_id
    WHERE o3.customer_id = v_customer_id
      AND bp.status IN ('Active', 'Overdue')
      AND bp.bnpl_id <> COALESCE(NEW.bnpl_id, -1)
  ) t;

  ----------------------------------------------------------------------
  -- Add the NEW BNPL's order amount too (because NEW is going to be active debt)
  ----------------------------------------------------------------------
  v_outstanding_debt := v_outstanding_debt + COALESCE((
    SELECT (SUM(oi3.quantity * oi3.final_price_at_order_time) * (1 - v_loyalty_discount))::numeric(14,2)
    FROM order_item oi3
    WHERE oi3.order_id = NEW.order_id
  ), 0);

  ----------------------------------------------------------------------
  --Enforce limit
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

-- 10 - The feedback's length should be less than 800 chars
ALTER TABLE feedback
ADD CONSTRAINT chk_feedback_comment_len
CHECK (comment_text IS NULL OR char_length(comment_text) <= 800);