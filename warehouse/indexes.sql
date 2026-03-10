USE prosper_dw;

-- Index on Loan Status for faster filtering in reports
CREATE INDEX idx_loan_status ON fact_loans(loan_status);

-- Index on Date to speed up time-series analysis
CREATE INDEX idx_origination_date ON fact_loans(loan_origination_date);

-- Index on Prosper Rating for risk analysis
CREATE INDEX idx_rating ON fact_loans(prosper_rating);