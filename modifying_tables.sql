-- adding balance column to walllet
ALTER TABLE Wallet
ADD COLUMN balance DECIMAL(15,2) NOT NULL DEFAULT 0.00;



-- adding order_id to wallet_transaction
ALTER TABLE Wallet_transaction
ADD COLUMN order_id INT,
ADD CONSTRAINT fk_wallet_transaction_order
    FOREIGN KEY (order_id) REFERENCES Ordere(order_id) ON DELETE SET NULL;



-- adding VAT_exemption_percent to Customer
ALTER TABLE Customer 
ADD COLUMN VAT_exemption_percent NUMERIC(3,2);



-- changing the allowed delivery method(shipping method) values according to the dataset data
ALTER TABLE shipment 
DROP CONSTRAINT shipment_delivery_type_check;

ALTER TABLE shipment
ADD CONSTRAINT shipment_delivery_type_check 
CHECK (delivery_type IN ('Ordinary', 'Express', 'Same_Day'));

ALTER TABLE shipment 
ALTER COLUMN delivery_type SET DEFAULT 'Ordinary';



-- changing the allowed transport_mothod(ship mode) values according to the dataset data
ALTER TABLE shipment 
DROP CONSTRAINT shipment_transport_method_check;

ALTER TABLE shipment
ADD CONSTRAINT shipment_transport_method_check 
CHECK (transport_method IN ('Ground', 'Air (Post)', 'Air (Freight)'));



-- changing the allowed packaging_material and packaging_size values according to the dataset data 
ALTER TABLE shipment 
DROP CONSTRAINT shipment_check;

ALTER TABLE shipment
ADD CONSTRAINT shipment_check 
CHECK (
    (packaging_type = 'Box'
      AND packaging_size IN ('Small', 'Medium', 'Large')
      AND packaging_material IS NULL)
    OR
    (packaging_type = 'Envelope'
      AND packaging_size IN ('Small', 'Large')
      AND (packaging_material IS NULL OR packaging_material = 'Bubble'))
);



-- changing the allowed customer_type(customer segment) values according to the dataset data
ALTER TABLE Customer 
DROP CONSTRAINT customer_customer_type_check;

ALTER TABLE Customer
ADD CONSTRAINT customer_customer_type_check 
CHECK (customer_type IS NULL OR customer_type IN ('Consumer', 'Small Business', 'Home Office', 'Corporate'));



-- changing the allowed payment_method values according to the dataset data
ALTER TABLE Ordere 
DROP CONSTRAINT IF EXISTS ordere_payment_method_check;

ALTER TABLE Ordere
ADD CONSTRAINT ordere_payment_method_check 
CHECK (payment_method IN ('Cash', 'Credit Card', 'Debit Card', 'BNPL', 'In-App Wallet'));



-- changing the allowed order_status values according to the dataset data
ALTER TABLE Ordere 
DROP CONSTRAINT IF EXISTS ordere_status_check;

ALTER TABLE Ordere
ADD CONSTRAINT ordere_status_check 
CHECK (status IN ('Unknown', 'Shipped', 'Received', 'Stocking', 'Pending Payment'));

