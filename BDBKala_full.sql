CREATE TABLE BDBKala_full (
  order_id INT,
  order_date DATE, //
  order_priority VARCHAR(20),
  order_quantity INT,
  order_status VARCHAR(30),
  payment_method VARCHAR(20),
  product_name VARCHAR(150),
  product_category VARCHAR(100),
  product_sub_category VARCHAR(100),
  unit_price NUMERIC(12, 2), 
  unit_cost NUMERIC(12, 2),
  discount NUMERIC(3, 5),
  shipping_address TEXT,
  shipping_method VARCHAR(20),
  ship_date DATE, //
  ship_mode VARCHAR(20),
  packaging VARCHAR(255),///
  shipping_cost NUMERIC(12, 2),
  region VARCHAR(100),
  city VARCHAR(100),
  zip_code VARCHAR(10),
  ratings INT,
  customer_segment VARCHAR(20),
  customer_name VARCHAR(200),
  customer_age INT,
  email VARCHAR(254),
  phone VARCHAR(20),
  gender VARCHAR(6),
  income NUMERIC(12, 2)
);


-- Import data by pgAdmin import CSV
