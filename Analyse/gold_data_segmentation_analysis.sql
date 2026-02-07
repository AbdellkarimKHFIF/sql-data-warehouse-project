/****************************************************************************************
File Name   : gold_data_segmentation_analysis.sql
Project     : Modern Data Warehouse with SQL Server
Layer       : Gold (Analytics & Business Intelligence)
Author      : Abdelkarim

Description :
    This script performs data segmentation analysis using the Gold layer.
    It categorizes products and customers into meaningful segments to
    support marketing, pricing, and customer strategy decisions.

    Segmentation Includes:
    - Product segmentation based on cost ranges
    - Customer segmentation based on spending behavior and lifecycle

    Business Use Cases:
    - Market segmentation
    - Targeted marketing campaigns
    - Customer value analysis
    - Product pricing strategy

****************************************************************************************/


/* =============================================================================
   PRODUCT COST SEGMENTATION
============================================================================= */

-- Segment products into predefined cost ranges
-- Helps understand product distribution across pricing tiers
WITH product_segment AS (
	SELECT
		product_key,
		product_name,
		cost,
		CASE
			WHEN cost < 100 THEN 'Below 100'
			WHEN cost BETWEEN 100 AND 500 THEN '100-500'
			WHEN cost BETWEEN 500 AND 1000 THEN '500-1000'
			ELSE 'Above 1000'
		END AS cost_range
	FROM gold.dim_products
)
SELECT 
	cost_range,
	COUNT(product_key) AS total_products
FROM product_segment
GROUP BY cost_range
ORDER BY total_products DESC;


/* =============================================================================
   CUSTOMER SPENDING & LIFECYCLE SEGMENTATION
============================================================================= */

-- Aggregate customer spending and lifecycle information
-- Used to classify customers based on value and engagement duration
WITH customer_spending AS (
	SELECT 
		c.customer_key,
		SUM(f.sales_amount) AS total_spending,
		MIN(order_date) AS first_order,
		MAX(order_date) AS last_order,
		DATEDIFF(month, MIN(order_date), MAX(order_date)) AS lifespan
	FROM gold.fact_sales f
	LEFT JOIN gold.dim_customers c
		ON f.customer_key = c.customer_key
	GROUP BY c.customer_key
)
SELECT 
	customer_segment,
	COUNT(customer_key) AS total_customers
FROM (
	SELECT 
		customer_key,
		CASE 
			-- High-value, long-term customers
			WHEN lifespan >= 12 AND total_spending > 5000 THEN 'VIP'

			-- Consistent customers with moderate spending
			WHEN lifespan >= 12 AND total_spending <= 5000 THEN 'Regular'

			-- Recently acquired or low-activity customers
			ELSE 'New'
		END AS customer_segment
	FROM customer_spending
)t
GROUP BY customer_segment
ORDER BY total_customers DESC;
