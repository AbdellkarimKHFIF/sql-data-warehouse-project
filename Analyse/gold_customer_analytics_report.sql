/****************************************************************************************
File Name   : gold_customer_analytics_report.sql
Project     : Modern Data Warehouse with SQL Server
Layer       : Gold (Analytics & Reporting)
Author      : Abdelkarim

Description :
    This script creates a customer analytics report view that consolidates
    key customer metrics, behaviors, and lifecycle indicators.

    The view is designed to support:
    - Customer 360° analysis
    - Segmentation and profiling
    - Sales performance tracking at customer level
    - CRM and marketing analytics
    - BI dashboards and executive reporting

    The report combines transactional data with customer master data
    to provide a unified and business-friendly customer view.

****************************************************************************************/


/* =============================================================================
   CUSTOMER REPORT VIEW
============================================================================= */

-- Create a reporting view that aggregates customer behavior and performance
CREATE VIEW gold.report_customers AS 

/* ---------------------------------------------------------------------------
   BASE QUERY
   - Joins fact sales with customer dimension
   - Derives customer age
   - Filters out records without valid order dates
--------------------------------------------------------------------------- */
WITH base_query AS (
	SELECT 
		f.order_number,
		f.product_key,
		f.order_date,
		f.sales_amount,
		f.quantity,
		c.customer_key,
		c.customer_number,
		CONCAT(c.first_name,' ',c.last_name) AS customer_name,
		DATEDIFF(year, c.birthdate, GETDATE()) AS age
	FROM gold.fact_sales f
	LEFT JOIN gold.dim_customers c
		ON c.customer_key = f.customer_key
	WHERE order_date IS NOT NULL
)

/* ---------------------------------------------------------------------------
   CUSTOMER AGGREGATION
   - Aggregates sales, orders, products, and lifecycle metrics per customer
--------------------------------------------------------------------------- */
, customer_aggregation AS (
	SELECT
		customer_key,
		customer_number,
		customer_name,
		age,
		COUNT(DISTINCT order_number) AS total_orders,
		SUM(sales_amount) AS total_sales,
		SUM(quantity) AS total_quantity,
		COUNT(DISTINCT product_key) AS total_products,
		MAX(order_date) AS last_order_date,
		DATEDIFF(month, MIN(order_date), MAX(order_date)) AS lifespan
	FROM base_query
	GROUP BY 
		customer_key,
		customer_number,
		customer_name,
		age
)

/* ---------------------------------------------------------------------------
   FINAL CUSTOMER REPORT
   - Adds customer segmentation, age groups, recency, and KPIs
--------------------------------------------------------------------------- */
SELECT 
	customer_key,
	customer_number,
	customer_name,
	age,

	-- Age grouping for demographic analysis
	CASE 
		WHEN age < 20 THEN 'Under 20'
		WHEN age BETWEEN 20 AND 49 THEN '20-49'
		ELSE '50 and above'
	END AS age_group,

	-- Customer segmentation based on lifecycle and spending
	CASE 
		WHEN lifespan >= 12 AND total_sales > 5000 THEN 'VIP'
		WHEN lifespan >= 12 AND total_sales <= 5000 THEN 'Regular'
		ELSE 'New'
	END AS customer_segment,

	last_order_date,

	-- Recency: months since last purchase
	DATEDIFF(month, last_order_date, GETDATE()) AS recency,

	-- Core customer KPIs
	total_orders,
	total_sales,
	total_quantity,
	total_products,
	lifespan,

	-- Average order value
	total_sales / total_orders AS avg_order_value,

	-- Average monthly spend (handles single-month customers)
	CASE
		WHEN lifespan = 0 THEN total_sales
		ELSE total_sales / lifespan
	END AS avg_monthly_spend
FROM customer_aggregation;
