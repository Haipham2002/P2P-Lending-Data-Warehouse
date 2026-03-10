# Data Quality Gate between the Transform and Load phases: This module will define a series of "Expectations" that the data must meet before it is allowed to be loaded into MySQL.

import pandas as pd

def validate_data(dim_borrower, dim_category, fact_loans):
    """
    Run a series of data quality checks. 
    Returns True if all checks pass, False otherwise.
    """
    print("--- Starting DATA QUALITY CHECKS ---")
    passed = True

    # Check 1: Primary Key Uniqueness (Fact Table)
    if fact_loans['loan_id'].duplicated().any():
        print("DQ Alert: Duplicate Loan IDs found in Fact table.")
        passed = False
    else:
        print("DQ Pass: No duplicate Loan IDs.")

    # Check 2: Null values in critical columns
    critical_cols = ['loan_id', 'loan_original_amount', 'category_id']
    if fact_loans[critical_cols].isnull().any().any():
        print(f"DQ Alert: Null values found in critical columns: {critical_cols}")
        passed = False
    else:
        print("DQ Pass: No nulls in critical columns.")

    # Check 3: Logical Range Checks (Income & Loan Amount)
    if (fact_loans['loan_original_amount'] <= 0).any():
        print("DQ Alert: Non-positive loan amounts detected.")
        passed = False
    
    # Check if monthly income is suspiciously high (e.g., > 1,000,000)
    # This helps catch data entry errors
    if (fact_loans['loan_original_amount'] > 50000).any(): 
        # Prosper's max loan is usually $35k-$40k, checking for >50k as a safety buffer
        print("DQ Warning: Loan amounts exceeding $50,000 detected.")

    # Check 4: Referential Integrity
    # Ensure every category_id in fact_loans exists in dim_category
    fact_categories = set(fact_loans['category_id'].unique())
    dim_categories = set(dim_category['category_id'].unique())
    
    if not fact_categories.issubset(dim_categories):
        missing = fact_categories - dim_categories
        print(f"DQ Alert: Category IDs {missing} in Fact table missing from Dimension table.")
        passed = False
    else:
        print("DQ Pass: Referential integrity confirmed.")

    return passed