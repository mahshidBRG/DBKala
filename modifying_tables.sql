-- adding balance column to walllet
ALTER TABLE Wallet
ADD COLUMN balance DECIMAL(15,2) NOT NULL DEFAULT 0.00;


-- adding order_id to wallet_transaction
ALTER TABLE Wallet_transaction
ADD COLUMN order_id INT,
ADD CONSTRAINT fk_wallet_transaction_order
    FOREIGN KEY (order_id) REFERENCES Ordere(order_id) ON DELETE SET NULL;

