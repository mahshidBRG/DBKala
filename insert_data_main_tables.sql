CREATE TABLE IF NOT EXISTS public.etl_error_log (
  error_id        bigserial PRIMARY KEY,
  occurred_at     timestamptz NOT NULL DEFAULT now(),
  target_table    text        NOT NULL,
  operation       text        NOT NULL,
  constraint_name text,
  sqlstate        text,
  error_message   text        NOT NULL,
  row_data        jsonb
);

CREATE OR REPLACE FUNCTION public.log_etl_error(
  p_target_table    text,
  p_operation       text,
  p_constraint_name text,
  p_sqlstate        text,
  p_error_message   text,
  p_row_data        jsonb
) RETURNS void
LANGUAGE plpgsql
AS $$
BEGIN
  INSERT INTO public.etl_error_log(
    target_table, operation, constraint_name, sqlstate, error_message, row_data
  )
  VALUES (
    p_target_table, p_operation, p_constraint_name, p_sqlstate, p_error_message, p_row_data
  );
END;
$$;
CREATE OR REPLACE PROCEDURE public.run_staging_load()
LANGUAGE plpgsql
AS $$
DECLARE
  r record;
  v_constraint text;
  v_sqlstate   text;
  v_msg        text;
BEGIN
  -------------------------------------------------------------------
  -- 1) Category (root)
  -------------------------------------------------------------------
  FOR r IN
    SELECT DISTINCT pp.category AS name
    FROM products_properties pp
    WHERE pp.category IS NOT NULL
      AND NOT EXISTS (
        SELECT 1
        FROM public.category c
        WHERE c.name = pp.category
          AND c.parent_category_id IS NULL
      )
  LOOP
    BEGIN
      INSERT INTO public.category(name) VALUES (r.name);
    EXCEPTION WHEN OTHERS THEN
      GET STACKED DIAGNOSTICS
        v_constraint = CONSTRAINT_NAME,
        v_sqlstate   = RETURNED_SQLSTATE,
        v_msg        = MESSAGE_TEXT;

      PERFORM public.log_etl_error(
        'category', 'INSERT root category',
        v_constraint, v_sqlstate, v_msg,
        to_jsonb(r)
      );
    END;
  END LOOP;

  -------------------------------------------------------------------
  -- 2) Category (sub-category child)
  -------------------------------------------------------------------
  FOR r IN
    SELECT DISTINCT
      pp.sub_category AS child_name,
      c.category_id   AS parent_id,
      pp.category     AS parent_name
    FROM products_properties pp
    JOIN public.category c
      ON c.name = pp.category
     AND c.parent_category_id IS NULL
    WHERE pp.sub_category IS NOT NULL
      AND NOT EXISTS (
        SELECT 1
        FROM public.category sub
        WHERE sub.name = pp.sub_category
          AND sub.parent_category_id = c.category_id
      )
  LOOP
    BEGIN
      INSERT INTO public.category(name, parent_category_id)
      VALUES (r.child_name, r.parent_id);
    EXCEPTION WHEN OTHERS THEN
      GET STACKED DIAGNOSTICS
        v_constraint = CONSTRAINT_NAME,
        v_sqlstate   = RETURNED_SQLSTATE,
        v_msg        = MESSAGE_TEXT;

      PERFORM public.log_etl_error(
        'category', 'INSERT sub-category',
        v_constraint, v_sqlstate, v_msg,
        to_jsonb(r)
      );
    END;
  END LOOP;

  -------------------------------------------------------------------
  -- 3) Branch_manager
  -------------------------------------------------------------------
  FOR r IN
    SELECT DISTINCT trim(bps.manager_name) AS name
    FROM branch_product_suppliers bps
    WHERE bps.manager_name IS NOT NULL
      AND trim(bps.manager_name) <> ''
      AND NOT EXISTS (
        SELECT 1
        FROM public.branch_manager bm
        WHERE bm.name = trim(bps.manager_name)
      )
  LOOP
    BEGIN
      INSERT INTO public.branch_manager(name) VALUES (r.name);
    EXCEPTION WHEN OTHERS THEN
      GET STACKED DIAGNOSTICS
        v_constraint = CONSTRAINT_NAME,
        v_sqlstate   = RETURNED_SQLSTATE,
        v_msg        = MESSAGE_TEXT;

      PERFORM public.log_etl_error(
        'branch_manager', 'INSERT manager',
        v_constraint, v_sqlstate, v_msg,
        to_jsonb(r)
      );
    END;
  END LOOP;

  -------------------------------------------------------------------
  -- 4) Address (shipping addresses from bdbkala_full)
  -------------------------------------------------------------------
  FOR r IN
    SELECT DISTINCT
      NULLIF(trim(s.shipping_address),'') AS recipient_address,
      NULLIF(trim(s.city),'') AS city,
      CASE
        WHEN NULLIF(trim(s.region),'') IN ('East','West','Central','South','North')
          THEN NULLIF(trim(s.region),'')
        ELSE NULL
      END AS region,
      NULLIF(trim(s.zip_code),'') AS zip_code
    FROM bdbkala_full s
    WHERE NULLIF(trim(s.shipping_address),'') IS NOT NULL
      AND NOT EXISTS (
        SELECT 1
        FROM public.address a
        WHERE a.recipient_address = NULLIF(trim(s.shipping_address),'')
          AND COALESCE(a.city,'') = COALESCE(NULLIF(trim(s.city),''),'')
          AND COALESCE(a.region,'') = COALESCE(
                CASE
                  WHEN NULLIF(trim(s.region),'') IN ('East','West','Central','South','North')
                    THEN NULLIF(trim(s.region),'')
                  ELSE NULL
                END,'')
          AND COALESCE(a.zip_code,'') = COALESCE(NULLIF(trim(s.zip_code),''),'')
      )
  LOOP
    BEGIN
      INSERT INTO public.address(recipient_address, city, region, zip_code)
      VALUES (r.recipient_address, r.city, r.region, r.zip_code);
    EXCEPTION WHEN OTHERS THEN
      GET STACKED DIAGNOSTICS
        v_constraint = CONSTRAINT_NAME,
        v_sqlstate   = RETURNED_SQLSTATE,
        v_msg        = MESSAGE_TEXT;

      PERFORM public.log_etl_error(
        'address', 'INSERT shipping address',
        v_constraint, v_sqlstate, v_msg,
        to_jsonb(r)
      );
    END;
  END LOOP;

  -------------------------------------------------------------------
  -- 5) Address (branch addresses from branch_product_suppliers)
  -------------------------------------------------------------------
  FOR r IN
    SELECT DISTINCT
      NULLIF(trim(bps.address),'') AS recipient_address
    FROM branch_product_suppliers bps
    WHERE NULLIF(trim(bps.address),'') IS NOT NULL
      AND NOT EXISTS (
        SELECT 1
        FROM public.address a
        WHERE a.recipient_address = NULLIF(trim(bps.address),'')
          AND a.city IS NULL AND a.region IS NULL AND a.zip_code IS NULL
      )
  LOOP
    BEGIN
      INSERT INTO public.address(recipient_address, city, region, zip_code)
      VALUES (r.recipient_address, NULL, NULL, NULL);
    EXCEPTION WHEN OTHERS THEN
      GET STACKED DIAGNOSTICS
        v_constraint = CONSTRAINT_NAME,
        v_sqlstate   = RETURNED_SQLSTATE,
        v_msg        = MESSAGE_TEXT;

      PERFORM public.log_etl_error(
        'address', 'INSERT branch address',
        v_constraint, v_sqlstate, v_msg,
        to_jsonb(r)
      );
    END;
  END LOOP;

  -------------------------------------------------------------------
  -- 6) Address (supplier addresses from branch_product_suppliers)
  -------------------------------------------------------------------
  FOR r IN
    SELECT DISTINCT
      NULLIF(trim(bps.supplier_address),'') AS recipient_address
    FROM branch_product_suppliers bps
    WHERE NULLIF(trim(bps.supplier_address),'') IS NOT NULL
      AND NOT EXISTS (
        SELECT 1
        FROM public.address a
        WHERE a.recipient_address = NULLIF(trim(bps.supplier_address),'')
          AND a.city IS NULL AND a.region IS NULL AND a.zip_code IS NULL
      )
  LOOP
    BEGIN
      INSERT INTO public.address(recipient_address, city, region, zip_code)
      VALUES (r.recipient_address, NULL, NULL, NULL);
    EXCEPTION WHEN OTHERS THEN
      GET STACKED DIAGNOSTICS
        v_constraint = CONSTRAINT_NAME,
        v_sqlstate   = RETURNED_SQLSTATE,
        v_msg        = MESSAGE_TEXT;

      PERFORM public.log_etl_error(
        'address', 'INSERT supplier address',
        v_constraint, v_sqlstate, v_msg,
        to_jsonb(r)
      );
    END;
  END LOOP;

  -------------------------------------------------------------------
  -- 7) Branch
  -------------------------------------------------------------------
  FOR r IN
    SELECT DISTINCT
      NULLIF(trim(bps.branch_name), '') AS name,
      NULLIF(trim(bps.phone), '')       AS phone,
      a.address_id                      AS address_id,
      m.manager_id                      AS manager_id,
      NULLIF(trim(bps.address), '')     AS address_text,
      NULLIF(trim(bps.manager_name),'') AS manager_name
    FROM branch_product_suppliers bps
    JOIN public.address a
      ON a.recipient_address = NULLIF(trim(bps.address), '')
     AND a.city IS NULL AND a.region IS NULL AND a.zip_code IS NULL
    JOIN public.branch_manager m
      ON m.name = NULLIF(trim(bps.manager_name), '')
    WHERE NULLIF(trim(bps.branch_name), '') IS NOT NULL
  LOOP
    BEGIN
      INSERT INTO public.branch(name, phone, address_id, manager_id)
      VALUES (r.name, r.phone, r.address_id, r.manager_id);
    EXCEPTION WHEN OTHERS THEN
      GET STACKED DIAGNOSTICS
        v_constraint = CONSTRAINT_NAME,
        v_sqlstate   = RETURNED_SQLSTATE,
        v_msg        = MESSAGE_TEXT;

      PERFORM public.log_etl_error(
        'branch', 'INSERT branch',
        v_constraint, v_sqlstate, v_msg,
        to_jsonb(r)
      );
    END;
  END LOOP;

  -------------------------------------------------------------------
  -- 8) Supplier
  -------------------------------------------------------------------
  FOR r IN
    SELECT DISTINCT
      NULLIF(trim(bps.supplier_name), '')    AS name,
      NULLIF(trim(bps.supplier_phone), '')   AS phone,
      a.address_id                           AS address_id,
      NULLIF(trim(bps.supplier_address), '') AS supplier_address
    FROM branch_product_suppliers bps
    LEFT JOIN public.address a
      ON a.recipient_address = NULLIF(trim(bps.supplier_address), '')
     AND a.city IS NULL AND a.region IS NULL AND a.zip_code IS NULL
    WHERE NULLIF(trim(bps.supplier_name), '') IS NOT NULL
  LOOP
    BEGIN
      INSERT INTO public.supplier(name, phone, address_id)
      VALUES (r.name, r.phone, r.address_id);
    EXCEPTION WHEN OTHERS THEN
      GET STACKED DIAGNOSTICS
        v_constraint = CONSTRAINT_NAME,
        v_sqlstate   = RETURNED_SQLSTATE,
        v_msg        = MESSAGE_TEXT;

      PERFORM public.log_etl_error(
        'supplier', 'INSERT supplier',
        v_constraint, v_sqlstate, v_msg,
        to_jsonb(r)
      );
    END;
  END LOOP;

END;
$$;


-----------------------------------------------------
---filing customer table
-----------------------------------------------------
CREATE OR REPLACE FUNCTION public.normalize_phone_us_e164(p text)
RETURNS text
LANGUAGE plpgsql
IMMUTABLE
AS $$
DECLARE
  s text;
  digits text;
BEGIN
  IF p IS NULL OR btrim(p) = '' THEN
    RETURN NULL;
  END IF;

  -- 1) remove extension (x123, ext 123, ext.123)
  s := regexp_replace(p, '\s*(ext\.?|x)\s*\d+.*$', '', 'i');

  -- 2) keep digits only
  digits := regexp_replace(s, '[^0-9]+', '', 'g');

  -- 3) handle international prefix 00
  IF digits ~ '^00' THEN
    digits := substr(digits, 3);
  END IF;

  -- 4) US normalization:
  -- 10 digits => +1##########
  -- 11 digits starting with 1 => +###########
  IF length(digits) = 10 THEN
    RETURN '+1' || digits;
  ELSIF length(digits) = 11 AND left(digits,1) = '1' THEN
    RETURN '+' || digits;
  END IF;

  -- otherwise invalid
  RETURN NULL;
END;
$$;
CREATE TABLE IF NOT EXISTS public.customer_reject_log (
  reject_id bigserial PRIMARY KEY,
  customer_name text,
  email text,
  phone_raw text,
  phone_norm text,
  reason text,
  created_at timestamp default now()
);

WITH base AS (
  SELECT DISTINCT ON (email, customer_name)
    NULLIF(btrim(customer_name), '') AS name,
    customer_age::int                AS age,
    income::numeric(12,2)            AS income,
    NULLIF(btrim(email), '')         AS email,
    phone::text                      AS phone_raw,
    NULLIF(btrim(gender), '')        AS gender_raw,
    NULLIF(btrim(customer_segment), '') AS customer_segment
  FROM public.BDBKala_full
  ORDER BY email, customer_name
),
x AS (
  SELECT *, public.normalize_phone_us_e164(phone_raw) AS phone_norm
  FROM base
),
validated AS (
  SELECT
    *,
    CASE
      WHEN name IS NULL THEN 'name is null/empty'
      WHEN email IS NULL THEN 'email is null/empty'
      WHEN email IS NOT NULL AND email !~ '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$' THEN 'invalid email'
      WHEN phone_raw IS NOT NULL AND btrim(phone_raw) <> '' AND phone_norm IS NULL THEN 'invalid phone'
      ELSE NULL
    END AS reject_reason
  FROM x
),
ins_reject AS (
  INSERT INTO public.customer_reject_log(customer_name,email,phone_raw,phone_norm,reason)
  SELECT name, email, phone_raw, phone_norm, reject_reason
  FROM validated
  WHERE reject_reason IS NOT NULL
  RETURNING 1
)
INSERT INTO public.customer
  (name, age, income, gender, email, phone, customer_type, customer_status, vat_exemption_percent)
SELECT
  name,
  age,
  income,
  CASE
    WHEN gender_raw ILIKE 'male'   THEN 'Male'
    WHEN gender_raw ILIKE 'female' THEN 'Female'
    ELSE NULL
  END,
  email,
  phone_norm,
  CASE
    WHEN customer_segment ILIKE '%corporate%' THEN 'Corporate'
    WHEN customer_segment ILIKE '%small%'     THEN 'Small Business'
    WHEN customer_segment ILIKE '%home%'      THEN 'Home Office'
    WHEN customer_segment ILIKE '%consumer%'  THEN 'Consumer'
    ELSE 'Consumer'
  END,
  CASE
    WHEN income IS NULL THEN 'New'
    WHEN income >= 100000 THEN 'VIP'
    WHEN income >= 40000  THEN 'Regular'
    ELSE 'New'
  END,
  CASE
    WHEN customer_segment ILIKE '%corporate%' THEN 1.00::numeric(3,2)
    WHEN customer_segment ILIKE '%small%'     THEN 0.50::numeric(3,2)
    WHEN customer_segment ILIKE '%home%'      THEN 0.20::numeric(3,2)
    ELSE 0.00::numeric(3,2)
  END
FROM validated
WHERE reject_reason IS NULL;

-----------------------------------------------------
---filing product table
-----------------------------------------------------
INSERT INTO public.product (name, specifications, vat_exemption_percent, category_id)
SELECT
  pp.product_name AS name,
  pp.attributes::jsonb AS specifications,

  -- 0.00 to 0.15 (0% to 15%), deterministic per product_name
  round(((abs(hashtext(pp.product_name)) % 16)::numeric) / 100, 2) AS vat_exemption_percent,

  child.category_id AS category_id
FROM products_properties pp
JOIN public.category parent
  ON parent.name = pp.category
 AND parent.parent_category_id IS NULL
JOIN public.category child
  ON child.name = pp.sub_category
 AND child.parent_category_id = parent.category_id
ON CONFLICT (product_id) DO NOTHING;

-----------------------------------------------------
---filing branch_product table
-----------------------------------------------------
ALTER TABLE branch_product Drop column stock_quantity;
INSERT INTO branch_product (branch_id, product_id, sale_price, discount)
SELECT
    b.branch_id,
    p.product_id,
    COALESCE(pr.avg_unit_price, s.min_supply_price * 1.30) AS sale_price,
    COALESCE(pr.avg_discount, 0.00) AS discount
FROM (
    SELECT branch_name, product_name, MIN(supply_price) AS min_supply_price
    FROM branch_product_suppliers
    GROUP BY branch_name, product_name
) s
JOIN branch b ON b.name = s.branch_name
JOIN product p ON p.name = s.product_name
LEFT JOIN (
    SELECT product_name, AVG(unit_price) AS avg_unit_price, AVG(discount) AS avg_discount
    FROM BDBKala_full
    GROUP BY product_name
) pr ON pr.product_name = s.product_name
WHERE NOT EXISTS (
    SELECT 1
    FROM branch_product bp
    WHERE bp.branch_id = b.branch_id
      AND bp.product_id = p.product_id
);
  -------------------------------------------------------------------
  -- Supply
  -------------------------------------------------------------------
  FOR r IN
    SELECT DISTINCT
      s.supplier_id        AS supplier_id,
      bp.branch_product_id AS branch_product_id,
      bps.lead_time_days   AS supply_time,
      bps.supply_price     AS cost_price
    FROM branch_product_suppliers bps
    JOIN public.supplier s
      ON s.name = NULLIF(trim(bps.supplier_name), '')
    JOIN public.branch b
      ON b.name = NULLIF(trim(bps.branch_name), '')
    JOIN public.product p
      ON p.name = NULLIF(trim(bps.product_name), '')
    JOIN public.branch_product bp
      ON bp.branch_id = b.branch_id
     AND bp.product_id = p.product_id
    WHERE bps.supply_price IS NOT NULL
      AND bps.lead_time_days IS NOT NULL
  LOOP
    BEGIN
      INSERT INTO public.supply (supplier_id, supply_time, cost_price, branch_product_id)
      VALUES (r.supplier_id, r.supply_time, r.cost_price, r.branch_product_id);
    EXCEPTION WHEN OTHERS THEN
      GET STACKED DIAGNOSTICS
        v_constraint = CONSTRAINT_NAME,
        v_sqlstate   = RETURNED_SQLSTATE,
        v_msg        = MESSAGE_TEXT;

      PERFORM public.log_etl_error(
        'supply', 'INSERT supply',
        v_constraint, v_sqlstate, v_msg,
        to_jsonb(r)
      );
    END;
  END LOOP;

END;
$$;
-------------------------------------------------------------------
  -- wallet
-------------------------------------------------------------------
WITH wb_dedup AS (
  SELECT
    lower(trim(customer_email)) AS email,
    max(wallet_balance::numeric(15,2)) AS balance
  FROM wallet_balances
  WHERE customer_email IS NOT NULL
  GROUP BY lower(trim(customer_email))
)
INSERT INTO public.wallet (customer_id, balance)
SELECT c.customer_id, d.balance
FROM wb_dedup d
JOIN public.customer c
  ON lower(trim(c.email)) = d.email
ON CONFLICT (customer_id) DO UPDATE
SET balance = EXCLUDED.balance;
-------------------------------------------------------------------
  -- order
-------------------------------------------------------------------
BEGIN;

-- disable triggers that block historical inserts
ALTER TABLE public.ordere DISABLE TRIGGER trg_enforce_order_date;
ALTER TABLE public.ordere DISABLE TRIGGER trg_order_status;
ALTER TABLE public.ordere DISABLE TRIGGER trg_prevent_order_date_update;
ALTER TABLE public.ordere DISABLE TRIGGER trg_small_business_priority;

DO $$
DECLARE
  r record;

  v_customer_id int;
  v_order_date date;

  v_priority text;
  v_status text;
  v_payment text;

  v_constraint text;
  v_sqlstate text;
  v_msg text;
BEGIN
  FOR r IN
    WITH base AS (
      SELECT DISTINCT ON (b.order_id)
        b.order_id,
        NULLIF(trim(b.order_date), '')      AS order_date_raw,
        NULLIF(trim(b.order_priority), '')  AS order_priority_raw,
        NULLIF(trim(b.order_status), '')    AS order_status_raw,
        NULLIF(trim(b.payment_method), '')  AS payment_method_raw,

        lower(NULLIF(trim(b.email), ''))    AS email,
        NULLIF(trim(b.phone), '')           AS phone,
        NULLIF(trim(b.customer_name), '')   AS customer_name
      FROM public.bdbkala_full b
      WHERE b.order_id IS NOT NULL
      ORDER BY b.order_id
    )
    SELECT * FROM base
  LOOP

    ------------------------------------------------------------
    -- 1) resolve customer_id (email > phone > name)
    ------------------------------------------------------------
    SELECT c.customer_id
      INTO v_customer_id
    FROM public.customer c
    WHERE
      (r.email IS NOT NULL AND r.email <> '' AND c.email = r.email)
      OR ( (r.email IS NULL OR r.email = '') AND r.phone IS NOT NULL AND c.phone = r.phone )
      OR ( (r.email IS NULL OR r.email = '') AND (r.phone IS NULL OR r.phone = '') AND r.customer_name IS NOT NULL AND c.name = r.customer_name )
    ORDER BY
      CASE
        WHEN r.email IS NOT NULL AND r.email <> '' AND c.email = r.email THEN 1
        WHEN r.phone IS NOT NULL AND c.phone = r.phone THEN 2
        ELSE 3
      END
    LIMIT 1;

    IF v_customer_id IS NULL THEN
      PERFORM public.log_etl_error(
        'ordere',
        'RESOLVE customer_id',
        'ordere_customer_id_fkey',
        'ETL_LOOKUP',
        'Customer not found for this order (by email/phone/name)',
        to_jsonb(r)
      );
      CONTINUE;
    END IF;

    ------------------------------------------------------------
    -- 2) parse order_date (safe)
    ------------------------------------------------------------
    v_order_date := NULL;

    IF r.order_date_raw IS NULL THEN
      -- if missing, skip (ordere.order_date is NOT NULL)
      PERFORM public.log_etl_error(
        'ordere',
        'VALIDATION order_date',
        NULL,
        'ETL_VALIDATION',
        'order_date is NULL/empty',
        to_jsonb(r)
      );
      CONTINUE;
    END IF;

    BEGIN
      -- supports: MM.DD.YYYY
      IF r.order_date_raw ~ '^\d{2}\.\d{2}\.\d{4}$' THEN
        v_order_date := to_date(r.order_date_raw, 'MM.DD.YYYY');

      -- supports: DD-MM-YYYY
      ELSIF r.order_date_raw ~ '^\d{2}-\d{2}-\d{4}$' THEN
        v_order_date := to_date(r.order_date_raw, 'DD-MM-YYYY');

      -- supports: YYYY-MM-DD
      ELSIF r.order_date_raw ~ '^\d{4}-\d{2}-\d{2}$' THEN
        v_order_date := to_date(r.order_date_raw, 'YYYY-MM-DD');

      ELSE
        RAISE EXCEPTION 'Unrecognized date format: %', r.order_date_raw;
      END IF;

    EXCEPTION WHEN OTHERS THEN
      PERFORM public.log_etl_error(
        'ordere',
        'PARSE order_date',
        NULL,
        SQLSTATE,
        SQLERRM,
        to_jsonb(r)
      );
      CONTINUE;
    END;

    ------------------------------------------------------------
    -- 3) map enums to your constraints
    ------------------------------------------------------------
    -- priority: Low/Medium/High/Urgent/Critical
    v_priority :=
      CASE
        WHEN r.order_priority_raw ILIKE 'low%'      THEN 'Low'
        WHEN r.order_priority_raw ILIKE 'med%'      THEN 'Medium'
        WHEN r.order_priority_raw ILIKE 'high%'     THEN 'High'
        WHEN r.order_priority_raw ILIKE 'urgent%'   THEN 'Urgent'
        WHEN r.order_priority_raw ILIKE 'critical%' THEN 'Critical'
        ELSE NULL
      END;

    IF v_priority IS NULL THEN
      PERFORM public.log_etl_error(
        'ordere',
        'MAP priority',
        'ordere_priority_check',
        'ETL_VALIDATION',
        format('Invalid order_priority: "%s"', COALESCE(r.order_priority_raw,'NULL')),
        to_jsonb(r)
      );
      CONTINUE;
    END IF;

    -- status: Shipped/Received/Stocking/Pending Payment
    v_status :=
      CASE
        WHEN r.order_status_raw ILIKE 'shipp%'   THEN 'Shipped'
        WHEN r.order_status_raw ILIKE 'receiv%'  THEN 'Received'
        WHEN r.order_status_raw ILIKE 'stock%'   THEN 'Stocking'
        WHEN r.order_status_raw ILIKE 'pending%' THEN 'Pending Payment'
        ELSE NULL
      END;

    IF v_status IS NULL THEN
      PERFORM public.log_etl_error(
        'ordere',
        'MAP status',
        'ordere_status_check',
        'ETL_VALIDATION',
        format('Invalid order_status: "%s"', COALESCE(r.order_status_raw,'NULL')),
        to_jsonb(r)
      );
      CONTINUE;
    END IF;

    -- payment_method: Cash/Credit Card/Debit Card/BNPL/In-App Wallet
    v_payment :=
      CASE
        WHEN r.payment_method_raw ILIKE 'cash%'        THEN 'Cash'
        WHEN r.payment_method_raw ILIKE 'credit%'      THEN 'Credit Card'
        WHEN r.payment_method_raw ILIKE 'debit%'       THEN 'Debit Card'
        WHEN r.payment_method_raw ILIKE 'bnpl%'        THEN 'BNPL'
        WHEN r.payment_method_raw ILIKE '%wallet%'     THEN 'In-App Wallet'
        ELSE NULL
      END;

    IF v_payment IS NULL THEN
      PERFORM public.log_etl_error(
        'ordere',
        'MAP payment_method',
        'ordere_payment_method_check',
        'ETL_VALIDATION',
        format('Invalid payment_method: "%s"', COALESCE(r.payment_method_raw,'NULL')),
        to_jsonb(r)
      );
      CONTINUE;
    END IF;

    ------------------------------------------------------------
    -- 4) insert (catch constraint errors & continue)
    ------------------------------------------------------------
    BEGIN
      INSERT INTO public.ordere(order_id, customer_id, order_date, status, priority, payment_method)
      VALUES (r.order_id, v_customer_id, v_order_date, v_status, v_priority, v_payment)
      ON CONFLICT (order_id) DO NOTHING;

    EXCEPTION WHEN OTHERS THEN
      GET STACKED DIAGNOSTICS
        v_constraint = CONSTRAINT_NAME,
        v_sqlstate   = RETURNED_SQLSTATE,
        v_msg        = MESSAGE_TEXT;

      PERFORM public.log_etl_error(
        'ordere',
        'INSERT ordere (history backfill)',
        v_constraint,
        v_sqlstate,
        v_msg,
        to_jsonb(r)
      );
      CONTINUE;
    END;

  END LOOP;
END;
$$;

-- re-enable triggers
ALTER TABLE public.ordere ENABLE TRIGGER trg_enforce_order_date;
ALTER TABLE public.ordere ENABLE TRIGGER trg_order_status;
ALTER TABLE public.ordere ENABLE TRIGGER trg_prevent_order_date_update;
ALTER TABLE public.ordere ENABLE TRIGGER trg_small_business_priority;

COMMIT;

-------------------------------------------------------------------
  -- order_item
--------------------------------------------------------------------
BEGIN;

ALTER TABLE public.order_item DISABLE TRIGGER trg_order_item_set_cost_price;
ALTER TABLE public.order_item DISABLE TRIGGER trg_set_final_price_at_order_time;

DO $$
DECLARE
  r record;

  v_qty int;

  v_constraint text;
  v_sqlstate   text;
  v_msg        text;
BEGIN
  FOR r IN
    WITH src AS (
      SELECT
        b.order_id,

        -- keep raw text, DON'T CAST here
        NULLIF(trim(b.order_quantity), '') AS quantity_raw,

        (b.unit_price * (1 - COALESCE(b.discount, 0)))::numeric(12,2) AS final_price_at_order_time,
        b.unit_cost::numeric(12,2) AS cost_price_at_order_time,

        trim(b.product_name)         AS product_name,
        trim(b.product_category)     AS product_category,
        trim(b.product_sub_category) AS product_sub_category
      FROM public.bdbkala_full b
      WHERE b.order_id IS NOT NULL
    ),
    picked_branch AS (
      SELECT DISTINCT ON (s.order_id, s.product_name, s.product_category, s.product_sub_category)
        s.*,
        bps.branch_name
      FROM src s
      JOIN public.branch_product_suppliers bps
        ON bps.product_name = s.product_name
       AND bps.category     = s.product_category
       AND bps.sub_category = s.product_sub_category
      ORDER BY s.order_id, s.product_name, s.product_category, s.product_sub_category, bps.branch_name
    ),
    resolved AS (
      SELECT
        pb.order_id,
        pb.quantity_raw,
        pb.final_price_at_order_time,
        pb.cost_price_at_order_time,
        bp.branch_product_id,
        pb.product_name, pb.product_category, pb.product_sub_category, pb.branch_name
      FROM picked_branch pb
      JOIN public.branch br
        ON br.name = pb.branch_name
      JOIN public.category c
        ON c.name = pb.product_category
       AND c.parent_category_id IS NULL
      JOIN public.category sc
        ON sc.name = pb.product_sub_category
       AND sc.parent_category_id = c.category_id
      JOIN public.product p
        ON p.name = pb.product_name
       AND p.category_id = sc.category_id
      JOIN public.branch_product bp
        ON bp.branch_id  = br.branch_id
       AND bp.product_id = p.product_id
    )
    SELECT * FROM resolved
  LOOP

    -- 1) validate quantity first (so we don't crash)
    IF r.quantity_raw IS NULL OR r.quantity_raw !~ '^-?\d+$' THEN
      PERFORM public.log_etl_error(
        'order_item',
        'VALIDATION order_quantity (not integer text)',
        NULL,
        'ETL_VALIDATION',
        format('Invalid integer text for order_quantity: "%s"', COALESCE(r.quantity_raw,'NULL')),
        to_jsonb(r)
      );
      CONTINUE;
    END IF;

    v_qty := r.quantity_raw::int;

    IF v_qty <= 0 THEN
      PERFORM public.log_etl_error(
        'order_item',
        'VALIDATION order_quantity (<=0)',
        'order_item_quantity_check',
        'ETL_VALIDATION',
        format('Invalid quantity (must be > 0). Got: %s', v_qty),
        to_jsonb(r)
      );
      CONTINUE;
    END IF;

    -- 2) try the insert; constraint/FK errors go to log table
    BEGIN
      INSERT INTO public.order_item(
        order_id, quantity, return_status,
        final_price_at_order_time, cost_price_at_order_time,
        branch_product_id
      )
      VALUES (
        r.order_id,
        v_qty,
        NULL,
        r.final_price_at_order_time,
        r.cost_price_at_order_time,
        r.branch_product_id
      )
      ON CONFLICT (order_id, branch_product_id) DO NOTHING;

    EXCEPTION WHEN OTHERS THEN
      GET STACKED DIAGNOSTICS
        v_constraint = CONSTRAINT_NAME,
        v_sqlstate   = RETURNED_SQLSTATE,
        v_msg        = MESSAGE_TEXT;

      PERFORM public.log_etl_error(
        'order_item',
        'INSERT order_item (history backfill)',
        v_constraint,
        v_sqlstate,
        v_msg,
        to_jsonb(r)
      );
      CONTINUE;
    END;

  END LOOP;
END;
$$;

ALTER TABLE public.order_item ENABLE TRIGGER trg_order_item_set_cost_price;
ALTER TABLE public.order_item ENABLE TRIGGER trg_set_final_price_at_order_time;

COMMIT;
-------------------------------------------------------------------
  -- feedback
--------------------------------------------------------------------
DO $$
DECLARE
  r record;

  v_constraint text;
  v_sqlstate   text;
  v_msg        text;
BEGIN
  FOR r IN
    WITH resolved AS (
      SELECT
        rv.order_id,
        TRUE AS is_public,
        NULLIF(b.ratings, '')::int AS rating,
        rv.comment AS comment_text,
        rv.image   AS image_string,
        bp.branch_product_id
      FROM reviews rv
      JOIN BDBKala_full b
        ON b.order_id = rv.order_id
       AND b.product_name = rv.product_name
       AND b.product_category = rv.product_category
       AND b.product_sub_category = rv.product_sub_category

      -- self-referencing category: category -> subcategory
      JOIN category c
        ON c.name = rv.product_category
       AND c.parent_category_id IS NULL
      JOIN category sc
        ON sc.name = rv.product_sub_category
       AND sc.parent_category_id = c.category_id

      -- product has name + sub_category_id
      JOIN product p
        ON p.name = rv.product_name
       AND p.category_id = sc.category_id

      -- branch_product has product_id
      JOIN branch_product bp
        ON bp.product_id = p.product_id
    )
    SELECT DISTINCT *
    FROM resolved
  LOOP
    BEGIN
      INSERT INTO public.feedback (
        order_id, is_public, rating, comment_text, image_string, branch_product_id
      )
      VALUES (
        r.order_id, r.is_public, r.rating, r.comment_text, r.image_string, r.branch_product_id
      );

    EXCEPTION WHEN OTHERS THEN
      GET STACKED DIAGNOSTICS
        v_constraint = CONSTRAINT_NAME,
        v_sqlstate   = RETURNED_SQLSTATE,
        v_msg        = MESSAGE_TEXT;

      PERFORM public.log_etl_error(
        'feedback', 'INSERT feedback',
        v_constraint, v_sqlstate, v_msg,
        to_jsonb(r)
      );
    END;
  END LOOP;
END;
$$;

-------------------------------------------------------------------
  -- shipment
--------------------------------------------------------------------
DO $$
DECLARE
  r record;
  v_constraint text;
  v_sqlstate   text;
  v_msg        text;
BEGIN
  FOR r IN
    WITH src AS (
      SELECT DISTINCT ON (b.order_id)
        b.order_id,
        b.shipping_address,
        b.shipping_method,
        b.ship_mode,
        b.ship_date,
        b.shipping_cost,
        b.packaging
      FROM BDBKala_full b
      WHERE b.order_id IS NOT NULL
      ORDER BY b.order_id
    ),
    parsed AS (
      SELECT
        s.order_id,
        a.address_id,
        CASE
          WHEN lower(s.shipping_method) IN ('same-day','same_day','sameday') THEN 'Same_Day'
          WHEN lower(s.shipping_method) = 'express' THEN 'Express'
          ELSE 'Ordinary'
        END::varchar(20) AS delivery_type,
        NULLIF(trim(s.ship_mode), '')::varchar(20) AS transport_method,

        CASE
          WHEN s.ship_date ~ '^\d{4}-\d{2}-\d{2}$' THEN to_date(s.ship_date, 'YYYY-MM-DD')
          WHEN s.ship_date ~ '^\d{2}-\d{2}-\d{4}$' THEN to_date(s.ship_date, 'DD-MM-YYYY')
          WHEN s.ship_date ~ '^\d{4}/\d{2}/\d{2}$' THEN to_date(s.ship_date, 'YYYY/MM/DD')
          WHEN s.ship_date ~ '^\d{2}/\d{2}/\d{4}$' THEN to_date(s.ship_date, 'DD/MM/YYYY')
          WHEN s.ship_date ~ '^\d{2}\.\d{2}\.\d{4}$' THEN to_date(s.ship_date, 'MM.DD.YYYY') -- ✅
          ELSE NULL
        END AS shipping_date,

        s.shipping_cost::numeric(12,2) AS shipping_cost,

        CASE
          WHEN s.packaging ILIKE '%envelope%' THEN 'Envelope'
          WHEN s.packaging ILIKE '%box%'      THEN 'Box'
          ELSE NULL
        END::varchar(20) AS packaging_type,

        split_part(
          s.packaging,
          ' ',
          array_length(string_to_array(s.packaging, ' '), 1)
        )::varchar(20) AS packaging_size,

        CASE
          WHEN s.packaging ILIKE 'bubble envelope%' THEN 'Bubble'
          ELSE NULL
        END::varchar(20) AS packaging_material,

        s.ship_date AS ship_date_raw,
        s.shipping_address AS shipping_address_raw,
        s.packaging AS packaging_raw
      FROM src s
      LEFT JOIN public.address a
        ON a.recipient_address = s.shipping_address
    )
    SELECT * FROM parsed
  LOOP
    BEGIN
      INSERT INTO public.shipment (
        shipment_id, order_id, address_id, delivery_type, transport_method,
        shipping_date, shipping_cost, packaging_type, packaging_size, packaging_material
      )
      VALUES (
        nextval('shipment_shipment_id_seq'),
        r.order_id, r.address_id, r.delivery_type, r.transport_method,
        r.shipping_date, r.shipping_cost, r.packaging_type, r.packaging_size, r.packaging_material
      );

    EXCEPTION WHEN OTHERS THEN
      GET STACKED DIAGNOSTICS
        v_constraint = CONSTRAINT_NAME,
        v_sqlstate   = RETURNED_SQLSTATE,
        v_msg        = MESSAGE_TEXT;

      PERFORM public.log_etl_error(
        'shipment', 'INSERT shipment',
        v_constraint, v_sqlstate, v_msg,
        to_jsonb(r)
      );
    END;
  END LOOP;
END;
$$;


CALL public.run_staging_load();