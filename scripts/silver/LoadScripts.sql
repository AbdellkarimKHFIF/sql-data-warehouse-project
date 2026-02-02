/****************************************************************************************
Project       : Modern Data Warehouse with SQL Server
Database      : DataWarehouse
Schema        : silver
Object Type  : Stored Procedure
Procedure    : silver.load_silver
Layer        : Silver (Cleaned & Standardized Data Layer)
Author       : Abdelkarim
Description  :
    This stored procedure loads and transforms data from the Bronze layer
    into the Silver layer. It applies data cleansing, standardization, and
    basic business rules to prepare data for analytical consumption.

    Key Transformations:
    - Deduplication using ROW_NUMBER()
    - Text standardization (TRIM, UPPER)
    - Code-to-description mapping (gender, marital status, product line)
    - Date validation and conversion
    - Data quality corrections (null handling, recalculated metrics)

    Load Strategy:
    - Full reload using TRUNCATE + INSERT
    - Deterministic and repeatable transformations
    - Source of truth remains in Bronze layer

Execution   :
    EXEC silver.load_silver;
****************************************************************************************/

CREATE OR ALTER PROCEDURE silver.load_silver
AS
BEGIN

    -- ===================================================
    -- Load CRM Customer Information (Silver Layer)
    -- ===================================================
    -- Cleans customer attributes, standardizes codes,
    -- and keeps only the latest record per customer

    TRUNCATE TABLE silver.crm_cust_info;

    INSERT INTO silver.crm_cust_info (
        cst_id,
        cst_key,
        cst_firstname,
        cst_lastname,
        cst_material_status,
        cst_gndr,
        cst_create_date
    )
    SELECT
        cst_id,
        cst_key,
        TRIM(cst_firstname) AS cst_firstname,
        TRIM(cst_lastname) AS cst_lastname,
        -- Normalize marital status codes
        CASE 
            WHEN UPPER(TRIM(cst_material_status)) = 'S' THEN 'Single'
            WHEN UPPER(TRIM(cst_material_status)) = 'M' THEN 'Married'
            ELSE 'n/a'
        END AS cst_material_status,
        -- Normalize gender codes
        CASE 
            WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
            WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
            ELSE 'n/a'
        END AS cst_gndr,
        cst_create_date
    FROM (
        SELECT 
            *,
            ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS Flag
        FROM bronze.crm_cust_info
        WHERE cst_id IS NOT NULL
    ) t
    WHERE Flag = 1;


    -- ===================================================
    -- Load CRM Product Information (Silver Layer)
    -- ===================================================
    -- Standardizes product identifiers, product lines,
    -- and derives product validity date ranges

    TRUNCATE TABLE silver.crm_prd_info;

    INSERT INTO silver.crm_prd_info (
        prd_id,
        cat_id,
        prd_key,
        prd_nm, 
        prd_cost,  
        prd_line,  
        prd_start_dt,
        prd_end_dt   
    )
    SELECT
        prd_id,
        -- Extract category identifier from product key
        REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
        -- Extract clean product key
        SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,
        prd_nm,
        ISNULL(prd_cost, 0) AS prd_cost,
        -- Normalize product line codes
        CASE UPPER(TRIM(prd_line))
            WHEN 'M' THEN 'Mountain'
            WHEN 'R' THEN 'Road'
            WHEN 'S' THEN 'Other Sales'
            WHEN 'T' THEN 'Touring'
            ELSE 'n/a'
        END AS prd_line,
        CAST(prd_start_dt AS DATE) AS prd_start_dt,
        -- Derive product end date using next start date
        CAST(
            LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) - 1 
            AS DATE
        ) AS prd_end_dt
    FROM bronze.crm_prd_info;


    -- ===================================================
    -- Load CRM Sales Details (Silver Layer)
    -- ===================================================
    -- Validates dates, corrects sales metrics,
    -- and enforces pricing consistency

    TRUNCATE TABLE silver.crm_sales_details;

    INSERT INTO silver.crm_sales_details (
        sls_ord_num,
        sls_prd_key,
        sls_cust_id,
        sls_order_dt,
        sls_ship_dt,
        sls_due_dt,
        sls_sales,
        sls_quantity,
        sls_price
    )
    SELECT
        sls_ord_num,
        sls_prd_key,
        sls_cust_id,
        -- Validate and convert order date
        CASE 
            WHEN sls_order_dt = 0 OR LEN(sls_order_dt) <> 8 THEN NULL
            ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
        END AS sls_order_dt,
        -- Validate and convert ship date
        CASE 
            WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) <> 8 THEN NULL
            ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
        END AS sls_ship_dt,
        -- Validate and convert due date
        CASE 
            WHEN sls_due_dt = 0 OR LEN(sls_due_dt) <> 8 THEN NULL
            ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
        END AS sls_due_dt,
        -- Ensure sales amount consistency
        CASE 
            WHEN sls_sales IS NULL 
              OR sls_sales <= 0 
              OR sls_sales <> sls_quantity * ABS(sls_price)
                THEN sls_quantity * ABS(sls_price)
            ELSE sls_sales 
        END AS sls_sales,
        sls_quantity,
        -- Derive unit price when missing or invalid
        CASE
            WHEN sls_price IS NULL OR sls_price <= 0 
                THEN sls_sales / NULLIF(sls_quantity, 0)
            ELSE sls_price
        END AS sls_price 
    FROM bronze.crm_sales_details;


    -- ===================================================
    -- Load ERP Customer Demographics (Silver Layer)
    -- ===================================================
    -- Cleans customer identifiers, validates birth dates,
    -- and standardizes gender values

    TRUNCATE TABLE silver.erp_cust_az12;

    INSERT INTO silver.erp_cust_az12 (
        cid,
        bdate,
        gen
    )
    SELECT 
        CASE
            WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
            ELSE cid
        END AS cid,
        CASE
            WHEN bdate > GETDATE() THEN NULL
            ELSE bdate
        END AS bdate,
        CASE 
            WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
            WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
            ELSE 'n/a'
        END AS gen
    FROM bronze.erp_cust_az12;


    -- ===================================================
    -- Load ERP Customer Location (Silver Layer)
    -- ===================================================
    -- Standardizes country names and cleans identifiers

    TRUNCATE TABLE silver.erp_loc_a101;

    INSERT INTO silver.erp_loc_a101 (
        cid,
        cntry
    )
    SELECT
        REPLACE(cid, '-', '') AS cid,
        CASE
            WHEN TRIM(cntry) = 'DE' THEN 'Germany'
            WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
            WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
            ELSE TRIM(cntry)
        END AS cntry
    FROM bronze.erp_loc_a101;


    -- ===================================================
    -- Load ERP Product Categories (Silver Layer)
    -- ===================================================
    -- Direct pass-through with structural standardization

    TRUNCATE TABLE silver.erp_px_cat_g1v2;

    INSERT INTO silver.erp_px_cat_g1v2 (
        id,
        cat,
        subcat,
        maintenance
    )
    SELECT
        id,
        cat,
        subcat,
        maintenance
    FROM bronze.erp_px_cat_g1v2;

END;
GO

-- Execute Silver Layer Load
EXEC silver.load_silver;
