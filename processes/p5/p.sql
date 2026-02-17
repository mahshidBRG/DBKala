-- Query: Delayed shipments
-- Output: order_id (and useful context fields)
-- Rules:
-- 1) Same_Day shipments are delayed if shipping_date != order_date
-- 2) Express shipments are delayed if shipping_date > order_date + 2 days

SELECT
  s.order_id,
  s.delivery_type,
  o.order_date,
  s.shipping_date,
  (s.shipping_date - o.order_date) AS delay_days
FROM shipment s
JOIN ordere o
  ON o.order_id = s.order_id
WHERE
  (
    s.delivery_type = 'Same_Day'
    AND s.shipping_date <> o.order_date
  )
  OR
  (
    s.delivery_type = 'Express'
    AND s.shipping_date > o.order_date + INTERVAL '2 days'
  )
ORDER BY delay_days DESC, s.order_id;
