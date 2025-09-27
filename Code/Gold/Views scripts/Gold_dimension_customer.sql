/*
	Created View customer-dimension by joining multiple tables.
	This view consists of all the avilable info about customers coming from the source.
*/

IF OBJECT_ID('gold.dim_customers', 'V') IS NOT NULL
    DROP VIEW gold.dim_customers;
GO

CREATE VIEW gold.dim_customers AS
SELECT 
	ROW_NUMBER()OVER(ORDER BY ci.cst_create_date, ci.cst_id) AS customer_key, --Surrogate Key
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



