-- View: Support team - order items with return requests that are still pending review
-- Includes all items whose return status is not finalized yet (i.e., still "Return Pending Review").
-- If your design allows return_status to be NULL for "not requested", we exclude NULLs here.

CREATE OR REPLACE VIEW vw_support_pending_returns AS
SELECT
  oi.order_id,
  oi.branch_product_id,
  o.customer_id,
  o.order_date,
  o.status        AS order_status,
  o.priority      AS order_priority,
  o.payment_method,

  -- Return-related info
  oi.return_status,
  rr.reason,
  rr.request_date,
  rr.review_results,
  rr.decision_date,

  -- Item / product info
  bp.product_id,
  p.name          AS product_name,
  bp.branch_id,
  b.name          AS branch_name,

  -- Pricing / quantity
  oi.quantity,
  oi.final_price_at_order_time
FROM order_item oi
JOIN ordere o
  ON o.order_id = oi.order_id
JOIN return_request rr
  ON rr.order_id = oi.order_id
 AND rr.branch_product_id = oi.branch_product_id
JOIN branch_product bp
  ON bp.branch_product_id = oi.branch_product_id
JOIN product p
  ON p.product_id = bp.product_id
LEFT JOIN branch b
  ON b.branch_id = bp.branch_id
WHERE oi.return_status = 'Return Pending Review';
