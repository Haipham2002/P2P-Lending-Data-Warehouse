-- Setup the Database
CREATE DATABASE IF NOT EXISTS prosper_dw;
USE prosper_dw;

-- Clean start (Optional but good for shared environments)
SET FOREIGN_KEY_CHECKS = 0;
DROP TABLE IF EXISTS fact_loans;
DROP TABLE IF EXISTS dim_borrower;
DROP TABLE IF EXISTS dim_listing_category;
SET FOREIGN_KEY_CHECKS = 1;

-- Dimension: Listing Category
-- category_id is BIGINT to match Pandas default integer handling
CREATE TABLE dim_listing_category (
    category_id BIGINT PRIMARY KEY,
    category_name VARCHAR(255) NOT NULL
);

-- Dimension: Borrower
-- We use BIGINT for borrower_id as it is our generated surrogate key
CREATE TABLE dim_borrower (
    borrower_id BIGINT PRIMARY KEY,
    occupation VARCHAR(255),
    employment_status VARCHAR(255),
    income_range VARCHAR(255),
    is_homeowner BOOLEAN,
    borrower_state VARCHAR(50)
);

-- Fact Table: Loans
-- This links everything together
CREATE TABLE fact_loans (
    loan_id VARCHAR(50) PRIMARY KEY,
    listing_number BIGINT,
    term INT,
    loan_status VARCHAR(50),
    borrower_rate FLOAT,
    prosper_rating VARCHAR(10),
    loan_original_amount FLOAT,
    monthly_loan_payment FLOAT,
    loan_origination_date DATE,
    category_id BIGINT,
    borrower_id BIGINT,
    CONSTRAINT fk_category FOREIGN KEY (category_id) REFERENCES dim_listing_category(category_id),
    CONSTRAINT fk_borrower FOREIGN KEY (borrower_id) REFERENCES dim_borrower(borrower_id)
);