CREATE TABLE Category (
    category_id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    parent_category_id INT,
    FOREIGN KEY (parent_category_id) REFERENCES Category(category_id)ON DELETE SET NULL ON UPDATE CASCADE,
    CHECK (parent_category_id <> category_id)    
);


CREATE TABLE Product (
    product_id SERIAL PRIMARY KEY,
    name VARCHAR(150) NOT NULL,
    specifications JSONB,
    VAT_exemption_percent NUMERIC(3,2),
    category_id INT NOT NULL,
    FOREIGN KEY (category_id) REFERENCES Category(category_id) ,
    CHECK (VAT_exemption_percent BETWEEN 0 AND 1)
);


CREATE TABLE Customer (
    customer_id SERIAL PRIMARY KEY,
    name VARCHAR(200),
    age INT,
    income NUMERIC(12,2),
    gender VARCHAR(6) CHECK(gender IN ('Male', 'Female')),
    email VARCHAR(254), 
    phone VARCHAR(20),
    customer_type VARCHAR(20) CHECK (customer_type IN ('Consumer','CompanyBuyer')),
    customer_status VARCHAR(20) CHECK (customer_status IN ('New','Regular','VIP'))
);


CREATE TABLE Address (
    address_id SERIAL PRIMARY KEY,
    recipient_address TEXT NOT NULL,
    city VARCHAR(100) NOT NULL,
    region VARCHAR(100) CHECK( region IN ('East', 'West', 'Central', 'South', 'North')) NOT NULL,
    zip_code VARCHAR(10) NOT NULL
);


CREATE TABLE Branch_manager (
    manager_id SERIAL PRIMARY KEY,
    name VARCHAR(150) NOT NULL
);


CREATE TABLE Branch (
    branch_id SERIAL PRIMARY KEY,
    name VARCHAR(100),
    phone VARCHAR(20),
    address_id INT NOT NULL,
    manager_id INT NOT NULL,
    FOREIGN KEY (address_id) REFERENCES Address(address_id),
    FOREIGN KEY (manager_id) REFERENCES Branch_manager(manager_id) 
);


CREATE TABLE Branch_product (
    branch_id INT NOT NULL,
    product_id INT NOT NULL,
    stock_quantity INT CHECK(stock_quantity >= 0),
    sale_price NUMERIC(12,2) NOT NULL CHECK(sale_price > 0),
    discount NUMERIC(3,2) CHECK(discount BETWEEN 0 AND 1),
    FOREIGN KEY (branch_id) REFERENCES Branch(branch_id) ON DELETE CASCADE,
    FOREIGN KEY (product_id) REFERENCES Product(product_id) ON DELETE CASCADE,
    PRIMARY KEY (branch_id, product_id)
);


CREATE TABLE Supplier (
    supplier_id SERIAL PRIMARY KEY,
    name  VARCHAR(100) NOT NULL,
    phone VARCHAR(20),
    address_id INT,
    FOREIGN KEY(address_id) REFERENCES Address(address_id) ON DELETE RESTRICT
);


CREATE TABLE supply (
    supplier_id INT NOT NULL,
    branch_id INT NOT NULL,
    product_id INT NOT NULL,
    supply_time INT NOT NULL,
    cost_price NUMERIC(12,2) CHECK(cost_price > 0),

    PRIMARY KEY (supplier_id, branch_id, product_id),

    FOREIGN KEY (supplier_id) REFERENCES Supplier(supplier_id) ON DELETE CASCADE,
    FOREIGN KEY (branch_id, product_id)
    REFERENCES branch_product(branch_id, product_id) ON DELETE CASCADE

);


CREATE TABLE Ordere (
    order_id SERIAL PRIMARY KEY,
    customer_id INT NOT NULL,
    order_date DATE NOT NULL DEFAULT CURRENT_DATE,
    status VARCHAR(30) NOT NULL CHECK(status IN ('Pending Payment','Shipped','Received','Pending Restock','Unknown')),
    priority VARCHAR(20) NOT NULL CHECK(priority IN ('Low','Medium','High','Urgent','Critical')) NOT NULL DEFAULT 'Low',
    payment_method VARCHAR(20) NOT NULL CHECK(payment_method IN('Cash','Credit Card','In-App Wallet','Debit Card','BNPL')),
    FOREIGN KEY (customer_id) REFERENCES Customer(customer_id)
);

CREATE TABLE order_item (
  order_id INT NOT NULL,
  branch_id INT NOT NULL,
  product_id INT NOT NULL,

  quantity INT NOT NULL CHECK (quantity > 0),

  return_status VARCHAR(30),
  CHECK (return_status IS NULL OR return_status IN ('Return Pending Review', 'Return Approved', 'Return Rejected')),

  final_price_at_order_time NUMERIC(12,2) NOT NULL CHECK (final_price_at_order_time >= 0),

  PRIMARY KEY (order_id, branch_id, product_id),

  -- Foreign key linking to orders
  CONSTRAINT fk_order_item_order
    FOREIGN KEY (order_id)
    REFERENCES ordere(order_id)
    ON DELETE CASCADE,

  -- Foreign key linking to branch_product (branch + product combination)
  CONSTRAINT fk_order_item_branch_product
    FOREIGN KEY (branch_id, product_id)
    REFERENCES Branch_product(branch_id, product_id)
    ON DELETE RESTRICT
);

CREATE TABLE BNPL_PLAN(
    bnpl_id SERIAL PRIMARY KEY,
    order_id INT UNIQUE NOT NULL REFERENCES Ordere(order_id),
    status VARCHAR(10) CHECK(status IN('Active', 'Settled', 'Overdue', 'Canceled')) DEFAULT 'Active'
);
CREATE TABLE Wallet(
    wallet_id SERIAL PRIMARY KEY,
    customer_id INT UNIQUE NOT NULL,
    FOREIGN KEY (customer_id) REFERENCES Customer(customer_id) ON DELETE CASCADE
);

CREATE TABLE Wallet_transaction(
    wallet_id INT NOT NULL REFERENCES wallet(wallet_id) ON DELETE CASCADE,
    transaction_sequence_number SERIAL,
    transaction_time TIMESTAMP NOT NULL DEFAULT NOW(),
    amount NUMERIC(14,2) NOT NULL CHECK (amount > 0),
    transaction_type VARCHAR(20) NOT NULL
      CHECK (transaction_type IN ('Deposit','Withdrawal','Payment')),
    PRIMARY KEY (wallet_id, transaction_sequence_number)
);


CREATE TABLE shipment (
  shipment_id SERIAL PRIMARY KEY,

  order_id INT NOT NULL UNIQUE
    REFERENCES ordere(order_id) ON DELETE CASCADE,

  address_id INT NOT NULL
    REFERENCES Address(address_id) ON DELETE RESTRICT,

  delivery_type VARCHAR(20) NOT NULL DEFAULT 'Normal'
    CHECK (delivery_type IN ('Normal','Custom','Same Day')),

  transport_method VARCHAR(20) NOT NULL
    CHECK (transport_method IN ('Ground','Air Post','Air Cargo')),

  shipping_date DATE NOT NULL,

  shipping_cost NUMERIC(12,2) NOT NULL CHECK (shipping_cost >= 0),

  packaging_type VARCHAR(20) NOT NULL
    CHECK (packaging_type IN ('Box','Envelope')),

  packaging_size VARCHAR(20),
  packaging_material VARCHAR(20),

  CHECK (
    (packaging_type = 'Box'
      AND packaging_size IN ('Small','Medium','Large')
      AND packaging_material IS NULL)
    OR
    (packaging_type = 'Envelope'
      AND packaging_size IN ('Small','Large')
      AND packaging_material IN ('Normal','Bubble'))
  )
);
CREATE TABLE feedback (
  order_id INT NOT NULL,
  branch_id INT NOT NULL,
  product_id INT NOT NULL,

  is_public BOOLEAN NOT NULL DEFAULT TRUE,
  rating INT NOT NULL CHECK (rating BETWEEN 1 AND 5),
  comment_text TEXT,
  image_string TEXT,

  PRIMARY KEY (order_id, branch_id, product_id),

  CONSTRAINT fk_feedback_order_item
    FOREIGN KEY (order_id, branch_id, product_id)
    REFERENCES order_item(order_id, branch_id, product_id)
    ON DELETE CASCADE
);
CREATE TABLE repayment (
  repayment_id SERIAL PRIMARY KEY,

  bnpl_id INT NOT NULL
    REFERENCES bnpl_plan(bnpl_id) ON DELETE CASCADE,
  wallet_id INT,
  transaction_sequence_number INT,

  amount NUMERIC(14,2) NOT NULL CHECK (amount > 0),
  date DATE NOT NULL,
  method VARCHAR(20) NOT NULL
    CHECK (method IN ('Cash','Credit Card','In-App Wallet','Debit Card')),
  CONSTRAINT fk_repayment_wallet_tx
    FOREIGN KEY (wallet_id, transaction_sequence_number)
    REFERENCES wallet_transaction(wallet_id, transaction_sequence_number)
    ON DELETE RESTRICT,

  CONSTRAINT chk_repayment_wallet_link
    CHECK (
      (method = 'In-App Wallet' AND wallet_id IS NOT NULL AND transaction_sequence_number IS NOT NULL)
      OR
      (method <> 'In-App Wallet' AND wallet_id IS NULL AND transaction_sequence_number IS NULL)
    )
);
CREATE TABLE return_request (
  order_id INT NOT NULL,
  branch_id INT NOT NULL,
  product_id INT NOT NULL,

  reason TEXT NOT NULL,
  review_results TEXT,
  request_date DATE NOT NULL,
  decision_date DATE,

  PRIMARY KEY (order_id, branch_id, product_id),

  CONSTRAINT fk_return_request_order_item
    FOREIGN KEY (order_id, branch_id, product_id)
    REFERENCES order_item(order_id, branch_id, product_id)
    ON DELETE CASCADE,

  -- desision date should be after requeste date
  CONSTRAINT chk_return_dates
    CHECK (decision_date IS NULL OR decision_date >= request_date)
);


