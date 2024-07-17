-- Create the Chargebacks table
CREATE TABLE Chargebacks (
    transaction_id NVARCHAR(50) PRIMARY KEY,
    customer_id NVARCHAR(50),
    transaction_date DATETIME,
    amount FLOAT,
    status NVARCHAR(50),
    city NVARCHAR(50),
    state NVARCHAR(50)
);

-- Create the UnsettledTransactions table
CREATE TABLE UnsettledTransactions (
    transaction_id NVARCHAR(50) PRIMARY KEY,
    customer_id NVARCHAR(50),
    transaction_date DATETIME,
    amount FLOAT,
    status NVARCHAR(50),
    city NVARCHAR(50),
    state NVARCHAR(50)
);
