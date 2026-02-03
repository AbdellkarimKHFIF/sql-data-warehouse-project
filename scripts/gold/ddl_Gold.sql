/****************************************************************************************
Project       : Modern Data Warehouse with SQL Server
Layer         : Gold (Analytics & Reporting Layer)
Author        : Abdelkarim
Description   :
    This script creates Gold-layer views following a star schema design.
    The Gold layer exposes business-ready, analytics-optimized datasets
    built from cleansed and conformed Silver-layer data.

    Objects created:
    - Dimension Views:
        * gold.dim_customers
        * gold.dim_products
    - Fact View:
        * gold.fact_sales

    These views are intended for consumption by BI tools, dashboards,
    and analytical queries.

Notes:
    - Surrogate keys are generated using ROW_NUMBER()
    - Gold layer contains no raw data, only curated datasets
****************************************************************************************/


-- ============================================================================
-- DIMENSION: Customers
-- Description:
--     Customer dimension combining CRM customer master data with
--     ERP demographic and location attributes.
-- ============================================================================
CREATE VIEW gold.dim_customers AS
	SELECT 
		ROW_NUMBER() OVER(ORDER BY cst_id) AS customer_key,      -- Surrogate key
		ci.cst_id AS customer_id,                                -- Business customer ID
		ci.cst_key AS customer_number,                           -- Customer reference key
		ci.cst_firstname AS first_name,
		ci.cst_lastname AS last_name,
		la.cntry AS country,                                     -- Customer country
		ci.cst_material_status AS marital_status,
		CASE 
			WHEN ci.cst_gndr != 'n/a' THEN ci.cst_gndr
			ELSE COALESCE(ca.gen,'n/a')
		END AS gender,                                           -- Consolidated gender logic
		ca.bdate AS birthdate,
		ci.cst_create_date AS create_date                        -- Customer creation date
	FROM silver.crm_cust_info ci
	LEFT JOIN silver.erp_cust_az12 ca
		ON ci.cst_key = ca.cid
	LEFT JOIN silver.erp_loc_a101 la
		ON ci.cst_key = la.cid;


-- ============================================================================
-- DIMENSION: Products
-- Description:
--     Product dimension enriched with category and maintenance data
--     from ERP sources. Only active products are included.
-- ============================================================================
CREATE VIEW gold.dim_products
AS
	SELECT 
		ROW_NUMBER() OVER(ORDER BY pn.prd_start_dt, pn.prd_key) AS product_key, -- Surrogate key
		pn.prd_id AS product_id,                                                 -- Business product ID
		pn.prd_key AS product_number,                                            -- Product reference key
		pn.prd_nm AS product_name,
		pn.cat_id AS category_id,
		pc.cat AS category,
		pc.subcat AS subcategory,
		pc.maintenance,
		pn.prd_cost AS cost,
		pn.prd_line AS product_line,
		pn.prd_start_dt AS start_date
	FROM silver.crm_prd_info pn
	LEFT JOIN silver.erp_px_cat_g1v2 pc
		ON pn.cat_id = pc.id
	WHERE prd_end_dt IS NULL;                                                   -- Active products only


-- ============================================================================
-- FACT: Sales
-- Description:
--     Sales fact view capturing transactional sales data.
--     Linked to customer and product dimensions using surrogate keys.
-- ============================================================================
CREATE VIEW gold.fact_sales
AS
	SELECT 
		sd.sls_ord_num AS order_number,           -- Sales order number
		pr.product_key,                           -- FK to product dimension
		cu.customer_key,                          -- FK to customer dimension
		sd.sls_order_dt AS order_date,
		sd.sls_ship_dt AS shipping_date,
		sd.sls_due_dt AS due_date,
		sd.sls_sales AS sales_amount,
		sd.sls_quantity AS quantity,
		sd.sls_price AS price
	FROM silver.crm_sales_details sd
	LEFT JOIN gold.dim_products pr
		ON sd.sls_prd_key = pr.product_number
	LEFT JOIN gold.dim_customers cu
		ON sd.sls_cust_id = cu.customer_id;
