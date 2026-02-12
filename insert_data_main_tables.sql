INSERT INTO Category (name, parent_category_id)
SELECT DISTINCT
    category,
    NULL
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