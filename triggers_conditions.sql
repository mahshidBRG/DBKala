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




