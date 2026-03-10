# Prosper P2P Lending Data Warehouse & ETL Pipeline

## 📌 Project Overview
An end-to-end Data Engineering pipeline that extracts raw P2P lending data, transforms it into a normalized **Star Schema**, and loads it into a **MySQL Data Warehouse**. This project enables efficient risk analysis and business reporting on over 113,000 loan records.

## 🏗 Architecture (Star Schema)
The data is organized into a Star Schema to optimize query performance and reduce redundancy:
- **Fact Table**: `fact_loans` (Loan amounts, rates, status, and dates).
- **Dimension Tables**: 
  - `dim_borrower`: Normalized borrower profiles (Occupation, Income, State).
  - `dim_listing_category`: Mapping of numeric category codes to readable names.



## 🛠 Tech Stack
- **Language**: Python 3.13
- **Data Manipulation**: Pandas
- **Database**: MySQL 8.0
- **ORM/Connection**: SQLAlchemy & PyMySQL

## 🚀 ETL Workflow
1. **Extract**: Loads 113,937 rows from `prosper_raw.csv`.
2. **Transform**: 
   - **Data Cleaning**: Handled missing values in `ProsperRating` and `Occupation`.
   - **Deduplication**: Identified and removed ~870 duplicate records based on `LoanKey` to maintain Primary Key integrity.
   - **Normalization**: Separated data into dimensions and a fact table.
   - **Standardization**: Converted dates to SQL-friendly `YYYY-MM-DD` format.
3. **Load**: Pushed data to MySQL in optimized chunks using SQLAlchemy.

## ⚠️ Key Technical Challenges Solved
- **Primary Key Integrity**: Handled `IntegrityError` by implementing a deduplication step in Python after discovering duplicate `LoanKeys` in the source data.
- **Relational Constraints**: Managed `Foreign Key` dependencies by ensuring Dimension tables are loaded prior to the Fact table.
- **Schema Mapping**: Aligned Python DataFrame headers (CamelCase) with MySQL table columns (snake_case) to ensure seamless loading.

## 📊 Sample Analysis
With this warehouse, we can now run complex queries like:
- **Risk Analysis**: Calculating default rates per credit grade.
- **Portfolio Growth**: Monthly trends of total loan volume.
- **Demographic Insights**: Identifying high-volume lending states.

## 💻 How to Setup
1. Clone the repository.
2. Install dependencies: `pip install -r requirements.txt`.
3. Run `warehouse/create_tables.sql` in your MySQL instance.
4. Update `config/db_config.py` with your credentials.
5. Run the orchestrator: `python scripts/main.py`.