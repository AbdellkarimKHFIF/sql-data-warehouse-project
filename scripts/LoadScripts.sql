/****************************************************************************************
Project       : Modern Data Warehouse with SQL Server
Database      : DataWarehouse
Schema        : bronze
Object Type  : Stored Procedure
Procedure    : bronze.load_bronze
Layer        : Bronze (Raw Data Ingestion)
Author       : Abdelkarim
Description  :
    This stored procedure orchestrates the end-to-end loading of the Bronze
    layer tables in the Data Warehouse. It performs a full reload of raw data
    from CRM and ERP CSV source files into their corresponding Bronze tables.

    The procedure includes:
    - Truncation of existing Bronze tables
    - Bulk loading of CSV files using BULK INSERT
    - Execution logging using PRINT statements
    - Load duration tracking at table and batch level
    - Centralized error handling using TRY...CATCH

    This procedure represents the first stage of the ETL pipeline and is
    designed to be reusable, auditable, and performance-optimized.

Execution   :
    EXEC bronze.load_bronze;
****************************************************************************************/

CREATE OR ALTER PROCEDURE bronze.load_bronze
AS
BEGIN
    -- Declare timing variables for performance monitoring
    DECLARE 
        @start_time        DATETIME,
        @end_time          DATETIME,
        @batch_start_time  DATETIME,
        @batch_end_time    DATETIME;

    BEGIN TRY
        -- Record batch start time
        SET @batch_start_time = GETDATE();

        PRINT '===================================================';
        PRINT 'Loading Bronze Layer';
        PRINT '===================================================';

        -- ===================================================
        -- Load CRM Source Tables
        -- ===================================================

        PRINT '---------------------------------------------------';
        PRINT 'Loading CRM Tables';
        PRINT '---------------------------------------------------';

        -- Load CRM Customer Information
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table : bronze.crm_cust_info';
        TRUNCATE TABLE bronze.crm_cust_info;

        PRINT '>> Inserting Data Into : bronze.crm_cust_info';
        BULK INSERT bronze.crm_cust_info
        FROM 'C:\Users\SQL Data\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );

        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' 
              + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) 
              + ' seconds';
        PRINT '>> -------------------';

        -- Load CRM Product Information
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table : bronze.crm_prd_info';
        TRUNCATE TABLE bronze.crm_prd_info;

        PRINT '>> Inserting Data Into : bronze.crm_prd_info';
        BULK INSERT bronze.crm_prd_info
        FROM 'C:\Users\SQL Data\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );

        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' 
              + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) 
              + ' seconds';
        PRINT '>> -------------------';

        -- Load CRM Sales Details
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table : bronze.crm_sales_details';
        TRUNCATE TABLE bronze.crm_sales_details;

        PRINT '>> Inserting Data Into : bronze.crm_sales_details';
        BULK INSERT bronze.crm_sales_details
        FROM 'C:\Users\SQL Data\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );

        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' 
              + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) 
              + ' seconds';
        PRINT '>> -------------------';

        -- ===================================================
        -- Load ERP Source Tables
        -- ===================================================

        PRINT '---------------------------------------------------';
        PRINT 'Loading ERP Tables';
        PRINT '---------------------------------------------------';

        -- Load ERP Customer Demographics
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table : bronze.erp_cust_az12';
        TRUNCATE TABLE bronze.erp_cust_az12;

        PRINT '>> Inserting Data Into : bronze.erp_cust_az12';
        BULK INSERT bronze.erp_cust_az12
        FROM 'C:\Users\SQL Data\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );

        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' 
              + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) 
              + ' seconds';
        PRINT '>> -------------------';

        -- Load ERP Customer Location
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table : bronze.erp_loc_a101';
        TRUNCATE TABLE bronze.erp_loc_a101;

        PRINT '>> Inserting Data Into : bronze.erp_loc_a101';
        BULK INSERT bronze.erp_loc_a101
        FROM 'C:\Users\SQL Data\sql-data-warehouse-project\datasets\source_erp\loc_a101.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );

        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' 
              + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) 
              + ' seconds';
        PRINT '>> -------------------';

        -- Load ERP Product Categories
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table : bronze.erp_px_cat_g1v2';
        TRUNCATE TABLE bronze.erp_px_cat_g1v2;

        PRINT '>> Inserting Data Into : bronze.erp_px_cat_g1v2';
        BULK INSERT bronze.erp_px_cat_g1v2
        FROM 'C:\Users\SQL Data\sql-data-warehouse-project\datasets\source_erp\px_cat_g1v2.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );

        SET @end_time = GETDATE();

        -- Record batch end time
        SET @batch_end_time = GETDATE();

        PRINT '===============================================';
        PRINT 'Loading Bronze Layer Is Completed Successfully';
        PRINT 'Total Load Duration: ' 
              + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) 
              + ' seconds';
        PRINT '===============================================';

    END TRY
    BEGIN CATCH
        -- Error handling and logging
        PRINT '===============================================';
        PRINT 'Error Occurred During Bronze Load';
        PRINT 'Error Message : ' + ERROR_MESSAGE();
        PRINT 'Error Number  : ' + CAST(ERROR_NUMBER() AS NVARCHAR);
        PRINT 'Error State   : ' + CAST(ERROR_STATE() AS NVARCHAR);
        PRINT '===============================================';
    END CATCH
END;
GO
