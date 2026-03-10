USE prosper_dw;

-- Default Rate and Average Interest by Prosper Rating
SELECT 
    prosper_rating,
    COUNT(*) AS total_loans,
    ROUND(AVG(borrower_rate) * 100, 2) AS avg_interest_rate_pct,
    ROUND(SUM(CASE WHEN loan_status = 'Defaulted' THEN 1 ELSE 0 END) / COUNT(*) * 100, 2) AS default_rate_pct
FROM fact_loans
GROUP BY prosper_rating
ORDER BY avg_interest_rate_pct DESC;