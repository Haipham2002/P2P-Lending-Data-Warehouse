# Prosper P2P Lending: Data Warehouse & ETL Pipeline

An end-to-end Data Engineering project that extracts, transforms, and loads over 113,000 P2P lending records into a containerized MySQL Star Schema.

## Project Overview
The goal of this project is to transform raw, flat financial data from Prosper (P2P Lending) into a structured Data Warehouse. This allows for complex analytical queries (e.g., ROI by Credit Grade, Borrower Risk Profiles) that are impossible or inefficient to run on raw CSV files.

[Image of an ETL pipeline architecture diagram from source to data warehouse]

---

## Data Architecture (Star Schema)
To optimize for analytical performance, the data was normalized from 81 columns into a **Star Schema**:

* **Fact Table (`fact_loans`):** Contains measurable, quantitative data about each loan (Amount, Rate, Monthly Payment).
* **Dimension Table (`dim_borrower`):** Contains descriptive attributes of the borrower (Occupation, State, Income Range).
* **Dimension Table (`dim_listing_category`):** A lookup table for the 20+ loan categories (Debt Consolidation, Business, etc.).

[Image of a star schema data model showing fact and dimension tables]

---

## Tech Stack
* **Language:** Python 3.13 (Pandas, SQLAlchemy)
* **Database:** MySQL 8.0 (Dockerized)
* **Infrastructure:** Docker & Docker Compose
* **Tools:** Git, MySQL Workbench

---

## Quick Start (How to Run)

### 1. Prerequisites
* [Docker Desktop](https://www.docker.com/products/docker-desktop/) installed.
* Python 3.x installed.

### 2. Prepare the Data
Due to file size limits, the raw dataset is not included in this repository. 
1. Download the `prosperLoanData.csv` from [Kaggle](https://www.kaggle.com/datasets/yash612/prosper-loan-data).
2. Place the file inside the `/data` folder of this project.

### 3. Setup the Environment
Clone the repository and start the database container. This will automatically run `sql/init.sql` to build the schema.
```bash
git clone [https://github.com/Haipham2002/P2P-Lending-Data-Warehouse.git](https://github.com/Haipham2002/P2P-Lending-Data-Warehouse.git)
cd P2P-Lending-Data-Warehouse
docker-compose up -d