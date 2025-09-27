/*
	1.Check for duplicates or inconsistent values, Joins can generate multiple values.
	If 1 column from table A matches with 2 Columns from table B.
	Then we will have the same customer with multiple adresses or gender or dob if data is incorrect.
    expectations -- No Result
*/

SELECT 
    COUNT(*)
FROM silver.crm_cust_info AS ci			-- Primary Table
LEFT JOIN 
silver.erp_cust_az12 as eci				-- Gives gender and birthdate info
    ON ci.cst_key = eci.cid
LEFT JOIN silver.erp_loc_a101 as ecc	-- Gives country info
    ON ci.cst_key = ecc.cid
GROUP BY ci.cst_key
HAVING COUNT(*) >2;


/*
	2. To check if we have multiple values for the same attribute from different tables.
        Expectation -- Only matching value, if unmatching values then needs to be handeled
*/

SELECT DISTINCT
    ci.cst_gndr, 
    eci.gen
FROM silver.crm_cust_info AS ci			-- Primary Table
LEFT JOIN 
silver.erp_cust_az12 as eci				-- Gives gender and birthdate info
    ON ci.cst_key = eci.cid
LEFT JOIN silver.erp_loc_a101 as ecc	-- Gives country info
    ON ci.cst_key = ecc.cid
ORDER BY ci.cst_gndr, eci.gen;


/*
    3. Duplicacy check for the sls_prd_key, can be done for other columns too
        Expectations -- No Result
*/

SELECT
    COUNT(*)
FROM
(
SELECT
    pn.prd_id,
    pn.sls_prd_key,
    pn.prd_nm,
    pn.prd_cat_id,
    pc.cat,
    pc.subcat
FROM silver.crm_prd_info pn
LEFT JOIN silver.erp_px_cat_g1v2 pc
    ON pn.prd_cat_id = pc.id
WHERE pn.prd_end_dt IS NULL)t
GROUP BY sls_prd_key
HAVING COUNT(*)  > 1;

/*
    4. Integirity Check
        Expectation - No Result
*/
SELECT
*
FROM gold.fact_sales AS s
LEFT JOIN gold.dim_customers AS c
    ON s.customer_key = c.customer_key
WHERE c.customer_key IS NULL;

