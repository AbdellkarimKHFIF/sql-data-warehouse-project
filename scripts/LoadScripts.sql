/****************************************************************************************
Project       : Modern Data Warehouse with SQL Server
Database      : DataWarehouse
Schema        : bronze
Layer         : Bronze (Raw Data Ingestion)
Author        : Abdelkarim
Description   :
    This script loads raw data into the Bronze layer tables using SQL Server
    BULK INSERT operations. It represents the ingestion phase of the ETL
    pipeline, where data is extracted from flat files (CSV) and stored
    in SQL Server with minimal processing.

    Key Characteristics:
    - Full reload strategy using TRUNCATE + BULK INSERT
    - No data transformation or validation at this stage
    - Data is ingested exactly as received from source systems
    - Optimized for performance using TABLOCK

    Source Systems:
    - CRM CSV extracts
    - ERP CSV extracts

    Notes:
    - CSV files must be accessible by SQL Server
    - FIRSTROW = 2 is used to skip header rows
****************************************************************************************/

-- Switch to the Data Warehouse database
USE DataWarehouse;
GO

-- ============================================================================
-- CRM SOURCE DATA INGESTION (BRONZE LAYER)
-- ============================================================================

-- Reload CRM Customer Information data
TRUNCATE TABLE bronze.crm_cust_info;
GO

BULK INSERT bronze.crm_cust_info
FROM 'C:\Users\SQL Data\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
WITH (
    FIRSTROW = 2,              -- Skip CSV header row
    FIELDTERMINATOR = ',',     -- Comma-separated values
    TABLOCK                     -- Improve bulk load performance
);
GO

-- Reload CRM Product Information data
TRUNCATE TABLE bronze.crm_prd_info;
GO

BULK INSERT bronze.crm_prd_info
FROM 'C:\Users\SQL Data\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
WITH (
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    TABLOCK
);
GO

-- Reload CRM Sales Details data
TRUNCATE TABLE bronze.crm_sales_details;
GO

BULK INSERT bronze.crm_sales_details
FROM 'C:\Users\SQL Data\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
WITH (
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    TABLOCK
);
GO

-- ============================================================================
-- ERP SOURCE DATA INGESTION (BRONZE LAYER)
-- ============================================================================

-- Reload ERP Customer Demographics data
TRUNCATE TABLE bronze.erp_cust_az12;
GO

BULK INSERT bronze.erp_cust_az12
FROM 'C:\Users\SQL Data\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
WITH (
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    TABLOCK
);
GO

-- Reload ERP Customer Location data
TRUNCATE TABLE bronze.erp_loc_a101;
GO

BULK INSERT bronze.erp_loc_a101
FROM 'C:\Users\SQL Data\sql-data-warehouse-project\datasets\source_erp\loc_a101.csv'
WITH (
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    TABLOCK
);
GO

-- Reload ERP Product Category data
TRUNCATE TABLE bronze.erp_px_cat_g1v2;
GO

BULK INSERT bronze.erp_px_cat_g1v2
FROM 'C:\Users\SQL Data\sql-data-warehouse-project\datasets\source_erp\px_cat_g1v2.csv'
WITH (
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    TABLOCK
);
GO

-- ============================================================================
-- End of Bronze Layer Data Ingestion
-- ============================================================================
