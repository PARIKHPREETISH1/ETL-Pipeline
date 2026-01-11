# ETL Pipeline using Python and Snowflake

## Overview
This project implements an end-to-end ETL pipeline using Python and Snowflake to consolidate purchase order, supplier invoice, supplier master, and weather data from multiple heterogeneous sources. The pipeline enables reconciliation of invoice amounts against quoted purchase orders and enriches transactions with external weather data.

## Data Sources
- CSV: Monthly purchase order line item files (41 files)
- XML: Supplier invoice data
- Postgres: Supplier master data
- Snowflake Marketplace: Cybersyn NOAA weather datasets
- TXT: Zip code to latitude/longitude mapping file

## Tech Stack
- Python
- Snowflake
- SQL
- psycopg2
- Snowflake Connector for Python

## Key Features
- Automated ingestion of multiple CSV files using Snowflake stages
- XML shredding into relational tables
- Push-down transformations using Snowflake SQL
- Materialized views for performant analytics
- Integration of third-party weather data using geospatial joins
- Dynamic table creation based on CSV schema

## Pipeline Steps
1. Load and consolidate purchase order CSV files
2. Compute purchase order totals (POAmount)
3. Parse and load supplier invoice XML data
4. Reconcile invoices against purchase orders
5. Extract supplier data from Postgres and load into Snowflake
6. Map supplier zip codes to nearest weather stations
7. Enrich transactions with daily high temperature data

## Outputs
- `purchase_orders_and_invoices` materialized view
- `supplier_zip_code_weather` view
- Final joined dataset with invoice discrepancies and weather context

## How to Run
1. Configure Snowflake and Postgres credentials
2. Install required Python libraries
3. Run the Jupyter notebook sequentially

## Notes
All transformations are executed in Snowflake for scalability and performance. Python is used strictly for orchestration and automation.
