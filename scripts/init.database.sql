/****************************************************************************************
Project       : Modern Data Warehouse with SQL Server
Database      : DataWarehouse
Layered Model : Bronze / Silver / Gold Architecture
Author        : Abdellkarim
Description   :
    This script initializes the core database structure for a modern data warehouse
    built on SQL Server. It creates the main database and defines the three logical
    layers used in the ETL and analytics workflow:

    - Bronze Layer : Raw data ingestion from source systems (as-is data)
    - Silver Layer : Cleaned, transformed, and standardized data
    - Gold Layer   : Business-ready, aggregated, and analytics-optimized data

    This layered architecture improves data quality, maintainability, and scalability.
****************************************************************************************/

-- Switch to the master database to create a new database
USE master;
GO

-- Create the Data Warehouse database
CREATE DATABASE DataWarehouse;
GO

-- Switch context to the newly created Data Warehouse
USE DataWarehouse;
GO

-- ============================================================================
-- Create Data Warehouse Schemas (Bronze / Silver / Gold)
-- ============================================================================

-- Bronze schema: stores raw data extracted from source systems
CREATE SCHEMA bronze;
GO

-- Silver schema: stores cleaned and transformed data
CREATE SCHEMA silver;
GO

-- Gold schema: stores business-ready data for reporting and analytics
CREATE SCHEMA gold;
GO
