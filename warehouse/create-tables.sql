CREATE DATABASE IF NOT EXISTS prosper_dw;
USE prosper_dw;

-- Drop existing tables to avoid "Already Exists" errors
DROP TABLE IF EXISTS fact_loans;
DROP TABLE IF EXISTS dim_borrower;
DROP TABLE IF EXISTS dim_listing_category;

-- 1. Create Dimensions (Must come first)
CREATE TABLE dim_listing_category (
    category_id INT PRIMARY KEY,
    category_name VARCHAR(100) NOT NULL
);

CREATE TABLE dim_borrower (
    borrower_id INT PRIMARY KEY,
    occupation VARCHAR(255),
    employment_status VARCHAR(100),
    income_range VARCHAR(100),
    is_homeowner BOOLEAN,
    borrower_state VARCHAR(50)
);

-- 2. Create Fact Table (Must come last)
CREATE TABLE fact_loans (
    loan_id VARCHAR(50) PRIMARY KEY,
    listing_number INT,
    term INT,
    loan_status VARCHAR(100),
    borrower_rate DECIMAL(10, 4),
    prosper_rating VARCHAR(10),
    loan_original_amount DECIMAL(15, 2),
    monthly_loan_payment DECIMAL(15, 2),
    loan_origination_date DATE,
    category_id INT,
    CONSTRAINT fk_category FOREIGN KEY (category_id) REFERENCES dim_listing_category(category_id)
);