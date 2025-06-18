CREATE TABLE transactions_v2 (
    customer_id        BIGINT,
    name               VARCHAR(64),
    surname            VARCHAR(64),
    gender             CHAR(10),
    birthdate          DATE,
    transaction_amount DECIMAL(12,2),
    date               DATE,
    merchant_name      VARCHAR(128),
    category           VARCHAR(64)
);
