/****************************************************************************************
File Name   : gold_performance_over_time.sql
Project     : Modern Data Warehouse with SQL Server
Layer       : Gold (Analytics & Reporting)
Author      : Abdelkarim

Description :
    This script focuses on time-series and performance analysis
    using data from the Gold layer.

    It provides insights into:
    - Monthly and yearly sales trends
    - Customer and quantity evolution over time
    - Running totals and moving averages
    - Product performance compared to historical averages
    - Year-over-year product sales comparison
    - Category contribution to overall sales

    These analyses are typically used for:
    - Executive dashboards
    - Trend analysis
    - Performance monitoring
    - Strategic planning

****************************************************************************************/


/* =============================================================================
   SALES PERFORMANCE OVER TIME (MONTHLY & YEARLY)
============================================================================= */

-- Analyze sales performance aggregated by year and month
-- Provides a high-level view of revenue, customer activity, and volume trends
SELECT 
	YEAR(order_date)  AS order_year,
	MONTH(order_date) AS order_month,
	SUM(sales_amount) AS total_sales,
	COUNT(DISTINCT customer_key) AS total_customers,
	SUM(quantity) AS total_quantity
FROM gold.fact_sales
WHERE order_date IS NOT NULL
GROUP BY YEAR(order_date), MONTH(order_date)
ORDER BY YEAR(order_date), MONTH(order_date);


/* =============================================================================
   RUNNING TOTAL & MOVING AVERAGE ANALYSIS
============================================================================= */

-- Analyze monthly total sales with running total and moving average price
-- Useful for trend smoothing and cumulative performance tracking
SELECT
	order_date,
	total_sales,
	SUM(total_sales) OVER (
		PARTITION BY order_date 
		ORDER BY order_date
	) AS running_total_sales,
	AVG(avg_price) OVER (
		PARTITION BY order_date 
		ORDER BY order_date
	) AS moving_average_price
FROM (
	SELECT 
		DATETRUNC(month, order_date) AS order_date,
		SUM(sales_amount) AS total_sales,
		AVG(price) AS avg_price
	FROM gold.fact_sales
	WHERE order_date IS NOT NULL
	GROUP BY DATETRUNC(month, order_date)
)t;


/* =============================================================================
   YEARLY PRODUCT PERFORMANCE ANALYSIS
============================================================================= */

-- Compare each product's yearly sales to:
-- 1) Its historical average sales
-- 2) Its previous year's sales (Year-over-Year analysis)
WITH yearly_product_sales AS (
	SELECT 
		YEAR(f.order_date) AS order_year,
		p.product_name,
		SUM(f.sales_amount) AS current_sales
	FROM gold.fact_sales f 
	LEFT JOIN gold.dim_products p 
		ON f.product_key = p.product_key
	WHERE f.order_date IS NOT NULL
	GROUP BY 
		YEAR(f.order_date),
		p.product_name
)
SELECT 
	order_year,
	product_name,
	current_sales,

	-- Average sales per product across all years
	AVG(current_sales) OVER (PARTITION BY product_name) AS avg_sales,

	-- Difference between current year and average sales
	current_sales 
		- AVG(current_sales) OVER (PARTITION BY product_name) AS diff_avg,

	-- Classification vs average performance
	CASE 
		WHEN current_sales 
			- AVG(current_sales) OVER (PARTITION BY product_name) > 0 THEN 'Above Avg'
		WHEN current_sales 
			- AVG(current_sales) OVER (PARTITION BY product_name) < 0 THEN 'Below Avg'
		ELSE 'Avg'
	END AS avg_change,

	-- Previous year sales (YoY comparison)
	LAG(current_sales) OVER (
		PARTITION BY product_name 
		ORDER BY order_year
	) AS py_sales,

	-- Year-over-year sales difference
	current_sales 
		- LAG(current_sales) OVER (
			PARTITION BY product_name 
			ORDER BY order_year
		) AS diff_py,

	-- Year-over-year performance classification
	CASE 
		WHEN current_sales 
			- LAG(current_sales) OVER (PARTITION BY product_name ORDER BY order_year) > 0 THEN 'Increase'
		WHEN current_sales 
			- LAG(current_sales) OVER (PARTITION BY product_name ORDER BY order_year) < 0 THEN 'Decrease'
		ELSE 'No Change'
	END AS py_change
FROM yearly_product_sales;


/* =============================================================================
   CATEGORY CONTRIBUTION ANALYSIS
============================================================================= */

-- Analyze how each product category contributes to overall sales
-- Shows total sales per category and percentage of overall revenue
WITH category_sales AS (
	SELECT
		category,
		SUM(sales_amount) AS total_sales
	FROM gold.fact_sales f
	LEFT JOIN gold.dim_products p
		ON p.product_key = f.product_key
	GROUP BY category
)
SELECT 
	category,
	total_sales,
	SUM(total_sales) OVER() AS overall_sales,
	CONCAT(
		ROUND(
			(CAST(total_sales AS FLOAT) / SUM(total_sales) OVER()) * 100,
		2),
		'%'
	) AS percentage_of_total
FROM category_sales
ORDER BY total_sales DESC;
