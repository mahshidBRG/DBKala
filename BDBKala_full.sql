CREATE TABLE BDBKala_full (
  order_id INT,
  order_date VARCHAR(255),
  order_priority VARCHAR(255),
  order_quantity VARCHAR(255),
  order_status VARCHAR(255),
  payment_method VARCHAR(255),
  product_name VARCHAR(255),
  product_category VARCHAR(255),
  product_sub_category VARCHAR(255),
  unit_price DECIMAL(18, 5),
  unit_cost DECIMAL(18, 5),
  discount DECIMAL(18, 5),
  shipping_address VARCHAR(255),
  shipping_method VARCHAR(255),
  ship_date VARCHAR(255),
  ship_mode VARCHAR(255),
  packaging VARCHAR(255),
  shipping_cost DECIMAL(18, 5),
  region VARCHAR(255),
  city VARCHAR(255),
  zip_code VARCHAR(10),
  ratings VARCHAR(10),
  customer_segment VARCHAR(255),
  customer_name VARCHAR(255),
  customer_age INT,
  email VARCHAR(255),
  phone VARCHAR(255),
  gender VARCHAR(255),
  income DECIMAL(18, 5)
);


-- Import data by pgAdmin import CSV
