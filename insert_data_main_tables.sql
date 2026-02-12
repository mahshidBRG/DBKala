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
