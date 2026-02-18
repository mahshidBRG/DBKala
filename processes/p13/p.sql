-- Query #13: Possible values for an attribute in a given category/subcategory
-- Inputs:
--   :category_name      (TEXT)
--   :sub_category_name  (TEXT)
--   :attribute_key      (TEXT)   -- e.g. 'processor' or 'cpu' or 'color'
--
-- Output:
--   distinct_attribute_value

SELECT DISTINCT
  p.specifications ->> :attribute_key AS distinct_attribute_value
FROM product p
JOIN category subc
  ON subc.category_id = p.category_id
JOIN category c
  ON c.category_id = subc.parent_category_id
WHERE c.name = :category_name
  AND subc.name = :sub_category_name
  AND p.specifications ? :attribute_key               -- key exists
  AND NULLIF(p.specifications ->> :attribute_key, '') IS NOT NULL
ORDER BY distinct_attribute_value;
