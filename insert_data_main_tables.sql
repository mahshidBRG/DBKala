INSERT INTO Category (name)
SELECT DISTINCT
    category
FROM products_properties
WHERE category IS NOT NULL
  AND NOT EXISTS (
      SELECT 1
      FROM Category c
      WHERE c.name = products_properties.category
        AND c.parent_category_id IS NULL   -- only root categories
  );

INSERT INTO Category (name, parent_category_id)
SELECT DISTINCT
    pp.sub_category,
    c.category_id
FROM products_properties pp
JOIN Category c
    ON c.name = pp.category
    AND c.parent_category_id IS NULL   -- only root categories
WHERE pp.sub_category IS NOT NULL
  AND NOT EXISTS (
      SELECT 1
      FROM Category sub
      WHERE sub.name = pp.sub_category
        AND sub.parent_category_id = c.category_id
  );

INSERT INTO Branch_manager(name)
SELECT DISTINCT trim(r.manager_name)
FROM branch_product_suppliers r
WHERE r.manager_name IS NOT NULL
  AND trim(r.manager_name) <> ''
  AND NOT EXISTS (
    SELECT 1
    FROM branch_manager bm
    WHERE bm.name = trim(r.manager_name)
  );

-- Insert unique shipping addresses from the main BDBkala_full staging table (city/region/zip taken from dataset; invalid region -> NULL)
INSERT INTO Address(recipient_address, city, region, zip_code)
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
    FROM Address a
    WHERE a.recipient_address = NULLIF(trim(s.shipping_address),'')
      AND COALESCE(a.city,'') = COALESCE(NULLIF(trim(s.city),''),'')
      AND COALESCE(a.region,'') = COALESCE(
            CASE
              WHEN NULLIF(trim(s.region),'') IN ('East','West','Central','South','North')
                THEN NULLIF(trim(s.region),'')
              ELSE NULL
            END,'')
      AND COALESCE(a.zip_code,'') = COALESCE(NULLIF(trim(s.zip_code),''),'')
  );

-- Insert unique branch addresses from branch_product_suppliers staging table (only address string exists -> city/region/zip stored as NULL)
INSERT INTO Address(recipient_address, city, region, zip_code)
SELECT DISTINCT
  NULLIF(trim(r.address),'') AS recipient_address,
  NULL, NULL, NULL
FROM branch_product_suppliers r
WHERE NULLIF(trim(r.address),'') IS NOT NULL
  AND NOT EXISTS (
    SELECT 1
    FROM Address a
    WHERE a.recipient_address = NULLIF(trim(r.address),'')
      AND a.city IS NULL
      AND a.region IS NULL
      AND a.zip_code IS NULL
  );

-- Insert unique supplier addresses from branch_product_suppliers staging table (only supplier_address string exists -> city/region/zip stored as NULL)
INSERT INTO Address(recipient_address, city, region, zip_code)
SELECT DISTINCT
  NULLIF(trim(r.supplier_address),'') AS recipient_address,
  NULL, NULL, NULL
FROM branch_product_suppliers r
WHERE NULLIF(trim(r.supplier_address),'') IS NOT NULL
  AND NOT EXISTS (
    SELECT 1
    FROM Address a
    WHERE a.recipient_address = NULLIF(trim(r.supplier_address),'')
      AND a.city IS NULL
      AND a.region IS NULL
      AND a.zip_code IS NULL
  );

-- Insert data into Branch table --
INSERT INTO Branch(name, phone, address_id, manager_id)
SELECT DISTINCT
  NULLIF(trim(r.branch_name), '') AS name,
  NULLIF(trim(r.phone), '') AS phone,
  a.address_id,
  m.manager_id
FROM branch_product_suppliers r
JOIN Address a
  ON a.recipient_address = NULLIF(trim(r.address), '')
 AND a.city IS NULL AND a.region IS NULL AND a.zip_code IS NULL
JOIN Branch_manager m
  ON m.name = NULLIF(trim(r.manager_name), '')
WHERE NULLIF(trim(r.branch_name), '') IS NOT NULL;


-- Insert into Supplier table --
INSERT INTO Supplier(name, phone, address_id)
SELECT DISTINCT
  NULLIF(trim(r.supplier_name), '') AS name,
  NULLIF(trim(r.supplier_phone), '') AS phone,
  a.address_id
FROM branch_product_suppliers r
LEFT JOIN Address a
  ON a.recipient_address = NULLIF(trim(r.supplier_address), '')
 AND a.city IS NULL AND a.region IS NULL AND a.zip_code IS NULL
WHERE NULLIF(trim(r.supplier_name), '') IS NOT NULL;


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
