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
CALL public.run_staging_load();


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
