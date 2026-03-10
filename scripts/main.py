# ETL PIPELINE

import os
from extract import extract_from_csv
from transform import transform_data
from load import load_to_mysql
from validate import validate_data
from pipeline_loging import setup_logging


def main():
    logger = setup_logging()
    logger.info("--- ETL PIPELINE STARTING ---")

    # PATH CONFIGURATION
    base_dir = os.path.dirname(os.path.abspath(__file__))
    raw_data_path = os.path.join(base_dir, '..', 'data', 'prosperLoanData.csv')

    try:
        # EXTRACT
        logger.info("Step 1: Extracting data from CSV...")
        raw_df = extract_from_csv(raw_data_path)
        
        if raw_df is None or raw_df.empty:
            logger.error("Extraction failed or CSV is empty. Aborting.")
            return

        # TRANSFORM
        logger.info("Step 2: Transforming data into Star Schema...")
        dim_borrower, dim_category, fact_loans = transform_data(raw_df)
        logger.info(f"Transformation complete. Prepared {len(fact_loans)} loan records.")

        # VALIDATE (The Data Quality Gate)
        logger.info("Step 3: Running Data Quality Checks...")
        if validate_data(dim_borrower, dim_category, fact_loans):
            logger.info("Data Quality Gate: PASSED.")
            
            # LOAD
            logger.info("Step 4: Loading data into MySQL Data Warehouse...")
            
            # Load Dimensions first to respect Foreign Key constraints
            load_to_mysql(dim_category, 'dim_listing_category', if_exists='replace')
            load_to_mysql(dim_borrower, 'dim_borrower', if_exists='replace')
            load_to_mysql(fact_loans, 'fact_loans', if_exists='replace')
            
            logger.info("--- ETL PIPELINE COMPLETED SUCCESSFULLY ---")
        else:
            logger.warning("Data Quality Gate: FAILED. Check logs for details. Load phase aborted.")

    except Exception as e:
        logger.error(f"Critical Pipeline Failure: {str(e)}", exc_info=True)

if __name__ == "__main__":
    main()