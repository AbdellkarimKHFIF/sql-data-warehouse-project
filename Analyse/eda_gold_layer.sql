/****************************************************************************************
File Name   : eda_gold_layer.sql
Project     : Modern Data Warehouse with SQL Server
Layer       : Gold (Analytics & Reporting)
Author      : Abdelkarim

Description :
    This script contains exploratory analysis and business KPI queries
    executed on the Gold layer of the data warehouse.

    The goal is to:
    - Understand the structure of the analytical model
    - Explore dimensions (customers, products, geography, dates)
    - Validate data coverage and quality
    - Compute key business measures (sales, quantity, orders, customers)

    These queries are typically used during:
    - Data validation
    - Business understanding
    - Pre-reporting analysis (before Power BI / Tableau dashboards)

****************************************************************************************/


/* =============================================================================
   DIMENSION EXPLORATION
   Purpose: Understand structure and content of dimension tables
============================================================================= */

-- Explore all tables available in the database
SELECT * 
FROM INFORMATION_SCHEMA.TABLES;


-- Explore columns of the customer dimension
-- Used to understand schema, data types, and available attributes
SELECT * 
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = 'dim_customers';


-- Explore the list of countries customers come from
-- Helps validate geographic coverage and standardization
SELECT DISTINCT country
FROM gold.dim_customers;


-- Explore product hierarchy: category, subcategory, and product
-- Useful for understanding product catalog structure
SELECT DISTINCT category, subcategory, product_name
FROM gold.dim_products
ORDER BY 1, 2, 3;


-- Explore the first and last order dates
-- Used to understand the historical range of available sales data
SELECT 
	MIN(order_date) AS first_order_date,
	MAX(order_date) AS last_order_date,
	DATEDIFF(YEAR, MIN(order_date), MAX(order_date)) AS sales_year
FROM gold.fact_sales;


-- Explore the youngest and oldest customers
-- Provides insight into customer age distribution
SELECT 
	MIN(birthdate) AS oldest_customer,
	DATEDIFF(YEAR, MIN(birthdate), GETDATE()) AS oldest_customer_age,
 	MAX(birthdate) AS youngest_customer,
	DATEDIFF(YEAR, MAX(birthdate), GETDATE()) AS youngest_customer_age
FROM gold.dim_customers;


/* =============================================================================
   MEASURE EXPLORATION
   Purpose: Analyze key business metrics and performance indicators
============================================================================= */

-- Calculate total sales revenue
SELECT 
	SUM(sales_amount) AS total_sales
FROM gold.fact_sales;


-- Calculate total quantity of items sold
SELECT 
	SUM(quantity) AS total_quantity
FROM gold.fact_sales;


-- Calculate average selling price
SELECT 
	AVG(price) AS avg_price
FROM gold.fact_sales;


-- Calculate total number of distinct orders
SELECT 
	COUNT(DISTINCT order_number) AS total_orders
FROM gold.fact_sales;


-- Calculate total number of products available
SELECT 
	COUNT(product_key) AS total_products
FROM gold.dim_products;


-- Calculate total number of customers
SELECT 
	COUNT(customer_key) AS total_customers
FROM gold.dim_customers;


-- Calculate number of customers who have placed at least one order
SELECT 
	COUNT(DISTINCT customer_key) AS total_customers
FROM gold.fact_sales;


-- Consolidated business KPI report
-- Produces a single result set with all key metrics
SELECT 'Total Sales' AS measure_name, SUM(sales_amount) AS measure_value FROM gold.fact_sales
UNION ALL
SELECT 'Total Quantity', SUM(quantity) FROM gold.fact_sales
UNION ALL 
SELECT 'Average Price', AVG(price) FROM gold.fact_sales
UNION ALL 
SELECT 'Total Orders', COUNT(DISTINCT order_number) FROM gold.fact_sales
UNION ALL 
SELECT 'Total Products', COUNT(product_name) FROM gold.dim_products
UNION ALL 
SELECT 'Total Customers', COUNT(customer_key) FROM gold.dim_customers;
