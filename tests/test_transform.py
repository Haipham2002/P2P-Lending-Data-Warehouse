# This script creates a small "mock" version of the Prosper data and checks if transform_data function correctly handles duplicates and nulls.

import pytest
import pandas as pd
from scripts.transform import transform_data

def test_deduplication():
    # 1. Create fake data with a duplicate LoanKey
    data = {
        'ListingKey': ['A', 'B'],
        'LoanKey': ['DUP123', 'DUP123'], # Duplicate!
        'ListingNumber': [1, 2],
        'Term': [36, 36],
        'LoanStatus': ['Current', 'Current'],
        'BorrowerRate': [0.1, 0.1],
        'ProsperRating (Alpha)': ['A', 'A'],
        'ListingCategory (numeric)': [1, 1],
        'BorrowerState': ['CA', 'CA'],
        'Occupation': ['Analyst', 'Analyst'],
        'EmploymentStatus': ['Employed', 'Employed'],
        'IsBorrowerHomeowner': [True, True],
        'IncomeRange': ['$50k', '$50k'],
        'StatedMonthlyIncome': [5000, 5000],
        'LoanOriginalAmount': [1000, 1000],
        'LoanOriginationDate': ['2023-01-01', '2023-01-01'],
        'MonthlyLoanPayment': [100, 100]
    }
    df = pd.DataFrame(data)
    
    # 2. Run the transformation
    _, _, fact_loans = transform_data(df)
    
    # 3. ASSERT: The final fact table should only have 1 row
    assert len(fact_loans) == 1
    assert fact_loans.iloc[0]['loan_id'] == 'DUP123'

def test_date_conversion():
    data = {
        'ListingKey': ['A'], 'LoanKey': ['L1'], 'ListingNumber': [1], 'Term': [36],
        'LoanStatus': ['Current'], 'BorrowerRate': [0.1], 'ProsperRating (Alpha)': ['A'],
        'ListingCategory (numeric)': [1], 'BorrowerState': ['CA'], 'Occupation': ['Analyst'],
        'EmploymentStatus': ['Employed'], 'IsBorrowerHomeowner': [True], 'IncomeRange': ['$50k'],
        'StatedMonthlyIncome': [5000], 'LoanOriginalAmount': [1000],
        'LoanOriginationDate': ['2023-08-15 00:00:00'], # Test timestamp format
        'MonthlyLoanPayment': [100]
    }
    df = pd.DataFrame(data)
    _, _, fact_loans = transform_data(df)
    
    # ASSERT: Should be a date object, not a string with time
    from datetime import date
    assert isinstance(fact_loans.iloc[0]['loan_origination_date'], date)


# Run 'python -m pytest' in the root folder