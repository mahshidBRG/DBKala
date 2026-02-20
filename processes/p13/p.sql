-- Query #13: Possible values for an attribute in a given category/subcategory
-- Inputs:
--   :category_name      (TEXT)
--   :sub_category_name  (TEXT)
--   :attribute_key      (TEXT)   -- e.g. 'processor' or 'cpu' or 'color'
--
-- Output:
--   distinct_attribute_value

CREATE OR REPLACE FUNCTION public.fn_attribute_values_by_category(
  p_category_name     text,
  p_sub_category_name text,
  p_attribute_key     text
)
RETURNS TABLE (
  distinct_attribute_value text
)
LANGUAGE sql
STABLE
AS $$
SELECT DISTINCT
  p.specifications ->> p_attribute_key AS distinct_attribute_value
FROM public.product p
JOIN public.category subc
  ON subc.category_id = p.category_id
JOIN public.category c
  ON c.category_id = subc.parent_category_id
WHERE c.name = p_category_name
  AND subc.name = p_sub_category_name
  AND p.specifications ? p_attribute_key
  AND NULLIF(p.specifications ->> p_attribute_key, '') IS NOT NULL
ORDER BY distinct_attribute_value;
$$;
