/****************************************************************************************
Project       : Modern Data Warehouse with SQL Server
Database      : DataWarehouse
Schema        : bronze
Layer         : Bronze (Raw Data Layer)
Author        : Abdelkarim
Description   :
    This script creates raw (bronze-layer) tables used for ingesting data from
    CRM and ERP source systems. The bronze layer stores data in its original
    structure with minimal transformation to preserve source fidelity.

    These tables serve as the entry point of the ETL pipeline and are later
    transformed into clean and standardized datasets in the Silver layer,
    and business-ready datasets in the Gold layer.

    Source Systems:
    - CRM System (Customer, Product, Sales data)
    - ERP System (Customer demographics, location, product categories)

    Notes:
    - Tables are dropped and recreated to support full reload scenarios
    - No business rules or transformations are applied at this stage
****************************************************************************************/

-- Switch to the Data Warehouse database
USE DataWarehouse;
GO

-- ============================================================================
-- CRM SOURCE TABLES (BRONZE LAYER)
-- ============================================================================

-- Drop and recreate CRM Customer Information table
IF OBJECT_ID('bronze.crm_cust_info', 'U') IS NOT NULL
    DROP TABLE bronze.crm_cust_info;
GO

-- Stores raw customer master data from CRM system
CREATE TABLE bronze.crm_cust_info (
    cst_id              INT,
    cst_key             NVARCHAR(50),
    cst_firstname       NVARCHAR(50),
    cst_lastname        NVARCHAR(50),
    cst_material_status NVARCHAR(50),
    cst_gndr            NVARCHAR(50),
    cst_create_date     DATE
);
GO

-- Drop and recreate CRM Product Information table
IF OBJECT_ID('bronze.crm_prd_info', 'U') IS NOT NULL
    DROP TABLE bronze.crm_prd_info;
GO

-- Stores raw product master data from CRM system
CREATE TABLE bronze.crm_prd_info (
    prd_id       INT,
    prd_key      NVARCHAR(50),
    prd_nm       NVARCHAR(50),
    prd_cost     INT,
    prd_line     NVARCHAR(50),
    prd_start_dt DATETIME,
    prd_end_dt   DATETIME
);
GO

-- Drop and recreate CRM Sales Details table
IF OBJECT_ID('bronze.crm_sales_details', 'U') IS NOT NULL
    DROP TABLE bronze.crm_sales_details;
GO

-- Stores raw transactional sales data from CRM system
CREATE TABLE bronze.crm_sales_details (
    sls_ord_num  NVARCHAR(50),
    sls_prd_key  NVARCHAR(50),
    sls_cust_id  INT,
    sls_order_dt INT,   -- Stored as INT (source system format)
    sls_ship_dt  INT,   -- Stored as INT (source system format)
    sls_due_dt   INT,   -- Stored as INT (source system format)
    sls_sales    INT,
    sls_quantity INT,
    sls_price    INT
);
GO

-- ============================================================================
-- ERP SOURCE TABLES (BRONZE LAYER)
-- ============================================================================

-- Drop and recreate ERP Customer Demographics table
IF OBJECT_ID('bronze.erp_cust_az12', 'U') IS NOT NULL
    DROP TABLE bronze.erp_cust_az12;
GO

-- Stores raw customer demographic data from ERP system
CREATE TABLE bronze.erp_cust_az12 (
    cid   NVARCHAR(50),
    bdate DATE,
    gen   NVARCHAR(50)
);
GO

-- Drop and recreate ERP Customer Location table
IF OBJECT_ID('bronze.erp_loc_a101', 'U') IS NOT NULL
    DROP TABLE bronze.erp_loc_a101;
GO

-- Stores raw customer location data from ERP system
CREATE TABLE bronze.erp_loc_a101 (
    cid   NVARCHAR(50),
    cntry NVARCHAR(50)
);
GO

-- Drop and recreate ERP Product Category table
IF OBJECT_ID('bronze.erp_px_cat_g1v2', 'U') IS NOT NULL
    DROP TABLE bronze.erp_px_cat_g1v2;
GO

-- Stores raw product category and maintenance data from ERP system
CREATE TABLE bronze.erp_px_cat_g1v2 (
    id          NVARCHAR(50),
    cat         NVARCHAR(50),
    subcat      NVARCHAR(50),
    maintenance NVARCHAR(50)
);
GO

-- ============================================================================
-- End of Bronze Layer Table Creation
-- ============================================================================
