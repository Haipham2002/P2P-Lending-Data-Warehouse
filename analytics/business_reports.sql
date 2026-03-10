USE prosper_dw;

-- Loan Volume by Category
SELECT 
    c.category_name,
    COUNT(f.loan_id) AS loan_count,
    SUM(f.loan_original_amount) AS total_amount_lent
FROM fact_loans f
JOIN dim_listing_category c ON f.category_id = c.category_id
GROUP BY c.category_name
ORDER BY total_amount_lent DESC;

-- Monthly Loan Origination Trend
SELECT 
    DATE_FORMAT(loan_origination_date, '%Y-%m') AS month,
    COUNT(*) AS loan_count,
    SUM(loan_original_amount) AS monthly_volume
FROM fact_loans
GROUP BY month
ORDER BY month ASC;