-- 6- This operation calculates the total VAT paid by a customer based on a fixed 10% rate.
--    Product and customer VAT exemptions are applied multiplicatively to the base rate,
--    and only finalized orders are included in the calculation.
CREATE OR REPLACE FUNCTION get_total_vat(p_customer_id INT)
RETURNS NUMERIC
AS $$
DECLARE
    total_vat NUMERIC;
BEGIN
    SELECT 
        COALESCE(SUM(
            oi.quantity *
            oi.final_price_at_order_time *
            0.10 *
            (1 - COALESCE(p.VAT_exemption_percent,0)) *
            (1 - COALESCE(c.VAT_exemption_percent,0))
        ), 0)
    INTO total_vat
    FROM Ordere o
    JOIN Customer c
        ON c.customer_id = o.customer_id
    JOIN order_item oi
        ON o.order_id = oi.order_id
    JOIN Branch_product bp
        ON bp.branch_product_id = oi.branch_product_id
    JOIN Product p
        ON p.product_id = bp.product_id
    WHERE o.customer_id = p_customer_id
      AND o.status IN ('Shipped','Received');

    RETURN total_vat;
END;
$$ LANGUAGE plpgsql;


-- Sample input: 100 (customer with id 100)
SELECT * FROM total_vat_paid_by_customer(100);

-- Output:
"total_vat_paid_by_customer"
40003.74285600
