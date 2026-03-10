# EXTRACT DATA: This module is responsible for loading the raw CSV data into a Pandas DataFrame.

import pandas as pd
import os

def extract_from_csv(file_path):
    """
    Extract data from the Prosper Loan CSV file.
    """
    print("--- Starting EXTRACT Phase ---")
    
    # Resolve absolute path to handle different execution environments
    absolute_path = os.path.abspath(file_path)
    
    if not os.path.exists(absolute_path):
        print(f"Error: File not found at {absolute_path}")
        return None

    try:
        # Loading the dataset
        df = pd.read_csv(absolute_path)
        print("Extraction Successful!")
        print(f"Data Shape: {df.shape[0]} rows, {df.shape[1]} columns")
        return df
    except Exception as e:
        print(f"Error during extraction: {str(e)}")
        return None

if __name__ == "__main__":
    # Path based on your project structure: data/prosper_raw.csv
    # When running from the 'scripts' folder, we go up one level then into 'data'
    test_path = os.path.join('..', 'prosper-data-warehouse', 'data', 'prosperLoanData.csv')
    extract_from_csv(test_path)