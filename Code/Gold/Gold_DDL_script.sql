/*
Script Purpose:
    This script creates views for the Gold layer in the data warehouse. 
        >> This consists if the dimension(Customers and Products), which describes the attribute of customers and products.
                and Facts(Sales), which shows the transactions(sales) that have taken place. 

        >> The data used in the views are from the silver layer and the dimensions have been used for facts. 
        >> Data quality checks have been performed using the gold_quality_check script and,
                all anomalies and incossitency have been handeled.

Usage:
        >> These views can be queried directly for analytics and reporting.
*/

-- Created dimension Customers
PRINT '======================================================================'  
PRINT 'Gold Layer DDL Script started '
GO

IF OBJECT_ID('gold.dim_customers', 'V') IS NOT NULL
    DROP VIEW gold.dim_customers;
GO

CREATE VIEW gold.dim_customers AS
SELECT 
	ROW_NUMBER()OVER(ORDER BY ci.cst_id) AS customer_key, --Surrogate Key
	ci.cst_id				AS customer_id,
	ci.cst_key				AS customer_number,
	ci.cst_firstname		AS firstname,
	ci.cst_lastname			AS lastname,
	CASE		-- Inconsistency in gender as it comes from two table so we have made a primary and secondary source
		WHEN ci.cst_gndr != 'n/a' THEN ci.cst_gndr		-- primary Source
		ELSE COALESCE(eci.gen, 'n/a')					-- Secondary Source
	END						AS gender,
	ecc.cntry				AS country,
	ci.cst_marital_status	AS marital_status,
	eci.bdate				AS birthdate,
	ci.cst_create_date		AS create_date
FROM silver.crm_cust_info AS ci			-- Primary Table
LEFT JOIN 
silver.erp_cust_az12 as eci				-- Gives gender and birthdate info
ON ci.cst_key = eci.cid
LEFT JOIN silver.erp_loc_a101 as ecc	-- Gives country info
ON ci.cst_key = ecc.cid
GO

PRINT '----------------------------------------------------------------------' + CHAR(13)
PRINT 'View for Dimension Customers Created' + CHAR(13)
GO

-- created dimension Products
IF OBJECT_ID('gold.dim_products', 'V') IS NOT NULL
    DROP VIEW gold.dim_products;
GO
CREATE VIEW gold.dim_products AS
SELECT
    ROW_NUMBER() OVER (ORDER BY pn.prd_start_dt, pn.sls_prd_key) AS product_key, -- Surrogate key(check in notes)
    pn.prd_id       AS product_id,
    pn.sls_prd_key  AS product_number,
    pn.prd_nm       AS product_name,
    pn.prd_cat_id   AS category_id,
    pc.cat          AS category,
    pc.subcat       AS subcategory,
    pc.maintenance  AS maintenance,
    pn.prd_cost     AS cost,
    pn.prd_line     AS product_line,
    pn.prd_start_dt AS start_date
FROM silver.crm_prd_info pn
LEFT JOIN silver.erp_px_cat_g1v2 pc
ON pn.prd_cat_id = pc.id
WHERE pn.prd_end_dt IS NULL; /* This filters out the history for thr products.
                                >> And keeps only the current products  */
GO
PRINT '----------------------------------------------------------------------' + CHAR(13)
PRINT 'View for Dimension Products Created' + CHAR(13)
GO


-- Created fact sales
IF OBJECT_ID('gold.fact_sales', 'V') IS NOT NULL
    DROP VIEW gold.fact_sales;
GO

CREATE VIEW gold.fact_sales AS
SELECT
    sd.sls_ord_num  AS order_number,
    pr.product_key  AS product_key,     -- Used the surrogate keys instead of the ID's to easily connect facts with dimension
    cu.customer_key AS customer_key,    -- Used the surrogate keys instead of the ID's to easily connect facts with dimension
    sd.sls_order_dt AS order_date,
    sd.sls_ship_dt  AS shipping_date,
    sd.sls_due_dt   AS due_date,
    sd.sls_sales    AS sales_amount,
    sd.sls_quantity AS quantity,
    sd.sls_price    AS price
FROM silver.crm_sales_details sd
LEFT JOIN gold.dim_products pr          -- Joined with view 
    ON sd.sls_prd_key = pr.product_number
LEFT JOIN gold.dim_customers cu         -- Joined with view 
    ON sd.sls_cust_id = cu.customer_id;
GO
PRINT '----------------------------------------------------------------------' + CHAR(13)
PRINT 'View for Fact sales Created' + CHAR(13)
PRINT '----------------------------------------------------------------------' + CHAR(13)
PRINT 'Gold Layer DDL Script Execution Ends '
PRINT '======================================================================' + CHAR(13)
GO