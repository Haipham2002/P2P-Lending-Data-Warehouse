# TRANSFORM: convert the raw 81-column dataset into a structured Star Schema. This involves cleaning the data, handling missing values, and splitting the dataframe into specific tables that reflect your SQL DDL.


# def transform_data(df):
#     print("--- Starting TRANSFORM Phase ---")
    
#     # Select core columns
#     required_columns = [
#         'ListingKey', 'LoanKey', 'ListingNumber', 'Term', 'LoanStatus', 
#         'BorrowerRate', 'ProsperRating (Alpha)', 'ListingCategory (numeric)', 
#         'BorrowerState', 'Occupation', 'EmploymentStatus', 'IsBorrowerHomeowner', 
#         'IncomeRange', 'StatedMonthlyIncome', 'LoanOriginalAmount', 
#         'LoanOriginationDate', 'MonthlyLoanPayment'
#     ]
#     df_clean = df[required_columns].copy()

#     # Basic Cleaning
#     df_clean['ProsperRating (Alpha)'] = df_clean['ProsperRating (Alpha)'].fillna('N/A')
#     df_clean['Occupation'] = df_clean['Occupation'].fillna('Unknown')
#     df_clean['EmploymentStatus'] = df_clean['EmploymentStatus'].fillna('Unknown')
#     df_clean['BorrowerState'] = df_clean['BorrowerState'].fillna('Unknown')
#     df_clean['LoanOriginationDate'] = pd.to_datetime(df_clean['LoanOriginationDate']).dt.date

#     # Dimension: dim_listing_category
#     category_map = {
#         0: 'Not Available', 1: 'Debt Consolidation', 2: 'Home Improvement', 
#         3: 'Business', 4: 'Personal Loan', 5: 'Student Use', 6: 'Auto', 
#         7: 'Other', 8: 'Baby&Adoption', 9: 'Boat', 10: 'Cosmetic Procedure',
#         11: 'Engagement Ring', 12: 'Green Loans', 13: 'Household Expenses',
#         14: 'Large Purchases', 15: 'Medical/Dental', 16: 'Motorcycle',
#         17: 'RV', 18: 'Taxes', 19: 'Vacation', 20: 'Wedding Loans'
#     }
#     dim_category = pd.DataFrame(list(category_map.items()), columns=['category_id', 'category_name'])

#     # Dimension: dim_borrower
#     dim_borrower = df_clean[[
#         'Occupation', 'EmploymentStatus', 'IncomeRange', 
#         'IsBorrowerHomeowner', 'BorrowerState'
#     ]].drop_duplicates().reset_index(drop=True)
    
#     # Rename to match SQL lowercase naming
#     dim_borrower.columns = ['occupation', 'employment_status', 'income_range', 'is_homeowner', 'borrower_state']
#     dim_borrower.insert(0, 'borrower_id', dim_borrower.index + 1)

#     # Fact Table: fact_loans (WITH DEDUPLICATION)
#     fact_loans = df_clean[[
#         'LoanKey', 'ListingNumber', 'Term', 'LoanStatus', 
#         'BorrowerRate', 'ProsperRating (Alpha)', 'LoanOriginalAmount', 
#         'MonthlyLoanPayment', 'LoanOriginationDate', 'ListingCategory (numeric)'
#     ]].copy()

#     # --- THE FIX: Remove duplicate LoanKeys before loading ---
#     fact_loans = fact_loans.drop_duplicates(subset=['LoanKey'])
    
#     fact_loans.columns = [
#         'loan_id', 'listing_number', 'term', 'loan_status', 
#         'borrower_rate', 'prosper_rating', 'loan_original_amount', 
#         'monthly_loan_payment', 'loan_origination_date', 'category_id'
#     ]

#     print("Transformation Successful!")
#     print(f"- Unique borrowers: {len(dim_borrower)}")
#     print(f"- Unique loans (Fact): {len(fact_loans)}")
    
#     return dim_borrower, dim_category, fact_loans


import pandas as pd
import numpy as np
import logging

logger = logging.getLogger(__name__)

def transform_data(df):
    logger.info("--- Starting TRANSFORM Phase ---")
    
    # Select core columns (Adding 'MemberKey' to link the tables)
    required_columns = [
        'LoanKey', 'MemberKey', 'ListingNumber', 'Term', 'LoanStatus', 
        'BorrowerRate', 'ProsperRating (Alpha)', 'ListingCategory (numeric)', 
        'BorrowerState', 'Occupation', 'EmploymentStatus', 'IsBorrowerHomeowner', 
        'IncomeRange', 'LoanOriginalAmount', 'LoanOriginationDate', 'MonthlyLoanPayment'
    ]
    
    # Ensure all columns exist, then create a copy
    df_clean = df[required_columns].copy()

    # Basic Cleaning & Null Handling
    df_clean['ProsperRating (Alpha)'] = df_clean['ProsperRating (Alpha)'].fillna('N/A')
    df_clean['Occupation'] = df_clean['Occupation'].fillna('Unknown')
    df_clean['EmploymentStatus'] = df_clean['EmploymentStatus'].fillna('Unknown')
    df_clean['BorrowerState'] = df_clean['BorrowerState'].fillna('Unknown')
    
    # Convert dates to actual date objects
    df_clean['LoanOriginationDate'] = pd.to_datetime(df_clean['LoanOriginationDate']).dt.date

    # Create numeric borrower_id mapping using MemberKey
    unique_members = df_clean['MemberKey'].unique()
    member_map = {key: i + 1 for i, key in enumerate(unique_members)}

    # Dimension: dim_listing_category
    category_map = {
        0: 'Not Available', 1: 'Debt Consolidation', 2: 'Home Improvement', 
        3: 'Business', 4: 'Personal Loan', 5: 'Student Use', 6: 'Auto', 
        7: 'Other', 8: 'Baby&Adoption', 9: 'Boat', 10: 'Cosmetic Procedure',
        11: 'Engagement Ring', 12: 'Green Loans', 13: 'Household Expenses',
        14: 'Large Purchases', 15: 'Medical/Dental', 16: 'Motorcycle',
        17: 'RV', 18: 'Taxes', 19: 'Vacation', 20: 'Wedding Loans'
    }
    dim_category = pd.DataFrame(list(category_map.items()), columns=['category_id', 'category_name'])

    # Dimension: dim_borrower
    dim_borrower = df_clean[[
        'MemberKey', 'Occupation', 'EmploymentStatus', 'IncomeRange', 
        'IsBorrowerHomeowner', 'BorrowerState'
    ]].drop_duplicates(subset=['MemberKey']).copy()
    
    # Map the unique numeric ID
    dim_borrower['borrower_id'] = dim_borrower['MemberKey'].map(member_map)
    
    # Select and rename columns for SQL
    dim_borrower = dim_borrower[[
        'borrower_id', 'Occupation', 'EmploymentStatus', 
        'IncomeRange', 'IsBorrowerHomeowner', 'BorrowerState'
    ]]
    dim_borrower.columns = ['borrower_id', 'occupation', 'employment_status', 'income_range', 'is_homeowner', 'borrower_state']

    # Fact Table: fact_loans
    fact_loans = df_clean[[
        'LoanKey', 'ListingNumber', 'Term', 'LoanStatus', 
        'BorrowerRate', 'ProsperRating (Alpha)', 'LoanOriginalAmount', 
        'MonthlyLoanPayment', 'LoanOriginationDate', 'ListingCategory (numeric)', 'MemberKey'
    ]].copy()

    # Apply the borrower mapping and deduplication
    fact_loans['borrower_id'] = fact_loans['MemberKey'].map(member_map)
    fact_loans = fact_loans.drop_duplicates(subset=['LoanKey'])
    
    # Rename and select final columns for SQL
    fact_loans = fact_loans.rename(columns={
        'LoanKey': 'loan_id',
        'ListingNumber': 'listing_number',
        'Term': 'term',
        'LoanStatus': 'loan_status',
        'BorrowerRate': 'borrower_rate',
        'ProsperRating (Alpha)': 'prosper_rating',
        'LoanOriginalAmount': 'loan_original_amount',
        'MonthlyLoanPayment': 'monthly_loan_payment',
        'LoanOriginationDate': 'loan_origination_date',
        'ListingCategory (numeric)': 'category_id'
    })
    
    fact_loans = fact_loans[[
        'loan_id', 'listing_number', 'term', 'loan_status', 'borrower_rate', 
        'prosper_rating', 'loan_original_amount', 'monthly_loan_payment', 
        'loan_origination_date', 'category_id', 'borrower_id'
    ]]

    logger.info("Transformation Successful!")
    logger.info(f"- Unique borrowers: {len(dim_borrower)}")
    logger.info(f"- Unique loans (Fact): {len(fact_loans)}")
    
    return dim_borrower, dim_category, fact_loans


if __name__ == "__main__":
    # Test block
    import os
    from extract import extract_from_csv
    
    # Adjust path if running locally for testing
    path = os.path.join('..', 'prosper-data-warehouse', 'data', 'prosperLoanData.csv')
    raw_data = extract_from_csv(path)
    
    if raw_data is not None:
        db, dc, fl = transform_data(raw_data)
        print("\nFact Loans Sample:")
        print(fl.head())