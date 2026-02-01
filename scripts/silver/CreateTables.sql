/****************************************************************************************
Project       : Modern Data Warehouse with SQL Server
Database      : DataWarehouse
Schema        : silver
Layer         : Silver (Cleaned & Standardized Data Layer)
Author        : Abdelkarim
Description   :
    This script creates Silver-layer tables used to store cleaned, standardized,
    and structured data derived from the Bronze layer.

    The Silver layer applies basic transformations such as:
    - Data type standardization
    - Structural alignment across source systems
    - Preparation for analytical and business-ready modeling

    These tables serve as an intermediate layer between raw ingestion (Bronze)
    and business consumption (Gold).

    Source Systems:
    - CRM System (Customer, Product, Sales data)
    - ERP System (Customer demographics, location, product categories)

    Notes:
    - Tables are dropped and recreated to support full reload scenarios
    - Light transformations and standardization occur in this layer
****************************************************************************************/

-- Switch to the Data Warehouse database
USE DataWarehouse;
GO

-- ============================================================================
-- CRM SOURCE TABLES (SILVER LAYER)
-- ============================================================================

-- Drop and recreate CRM Customer Information table
IF OBJECT_ID('silver.crm_cust_info', 'U') IS NOT NULL
    DROP TABLE silver.crm_cust_info;
GO

-- Stores cleaned and standardized customer master data
CREATE TABLE silver.crm_cust_info (
    cst_id              INT,
    cst_key             NVARCHAR(50),
    cst_firstname       NVARCHAR(50),
    cst_lastname        NVARCHAR(50),
    cst_material_status NVARCHAR(50),
    cst_gndr            NVARCHAR(50),
    cst_create_date     DATE,
    dwh_create_date DATETIME2 DEFAULT GETDATE()
);
GO

-- Drop and recreate CRM Product Information table
IF OBJECT_ID('silver.crm_prd_info', 'U') IS NOT NULL
    DROP TABLE silver.crm_prd_info;
GO

-- Stores cleaned and standardized product master data
CREATE TABLE silver.crm_prd_info (
    prd_id       INT,
    prd_key      NVARCHAR(50),
    prd_nm       NVARCHAR(50),
    prd_cost     INT,
    prd_line     NVARCHAR(50),
    prd_start_dt DATETIME,
    prd_end_dt   DATETIME,
    dwh_create_date DATETIME2 DEFAULT GETDATE()
);
GO

-- Drop and recreate CRM Sales Details table
IF OBJECT_ID('silver.crm_sales_details', 'U') IS NOT NULL
    DROP TABLE silver.crm_sales_details;
GO

-- Stores standardized transactional sales data
CREATE TABLE silver.crm_sales_details (
    sls_ord_num  NVARCHAR(50),
    sls_prd_key  NVARCHAR(50),
    sls_cust_id  INT,
    sls_order_dt INT,   -- To be converted to DATE in later transformations
    sls_ship_dt  INT,
    sls_due_dt   INT,
    sls_sales    INT,
    sls_quantity INT,
    sls_price    INT,
    dwh_create_date DATETIME2 DEFAULT GETDATE()
);
GO

-- ============================================================================
-- ERP SOURCE TABLES (SILVER LAYER)
-- ============================================================================

-- Drop and recreate ERP Customer Demographics table
IF OBJECT_ID('silver.erp_cust_az12', 'U') IS NOT NULL
    DROP TABLE silver.erp_cust_az12;
GO

-- Stores cleaned customer demographic data
CREATE TABLE silver.erp_cust_az12 (
    cid   NVARCHAR(50),
    bdate DATE,
    gen   NVARCHAR(50),
    dwh_create_date DATETIME2 DEFAULT GETDATE()
);
GO

-- Drop and recreate ERP Customer Location table
IF OBJECT_ID('silver.erp_loc_a101', 'U') IS NOT NULL
    DROP TABLE silver.erp_loc_a101;
GO

-- Stores standardized customer location data
CREATE TABLE silver.erp_loc_a101 (
    cid   NVARCHAR(50),
    cntry NVARCHAR(50),
    dwh_create_date DATETIME2 DEFAULT GETDATE()
);
GO

-- Drop and recreate ERP Product Category table
IF OBJECT_ID('silver.erp_px_cat_g1v2', 'U') IS NOT NULL
    DROP TABLE silver.erp_px_cat_g1v2;
GO

-- Stores standardized product category data
CREATE TABLE silver.erp_px_cat_g1v2 (
    id          NVARCHAR(50),
    cat         NVARCHAR(50),
    subcat      NVARCHAR(50),
    maintenance NVARCHAR(50),
    dwh_create_date DATETIME2 DEFAULT GETDATE()
);
GO

-- ============================================================================
-- End of Silver Layer Table Creation
-- ============================================================================
