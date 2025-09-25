/*
Cleaning and normalizing the data for table  silver.crm_prd_info
And then Inserting using truncate and insert method.
*/

TRUNCATE TABLE silver.crm_prd_info
INSERT INTO silver.crm_prd_info
(
	prd_id	,		
    prd_cat_id,
	sls_prd_key,
    prd_nm,
    prd_cost,
    prd_line,
    prd_start_dt,
    prd_end_dt
)
select 
prd_id,
REPLACE(SUBSTRING(prd_key, 1,5), '-', '_')  AS prd_cat_id, -- Genrating derived column prd_cat_id from prd_key and then replacing '-' with '_' because in other table '_' is used.
SUBSTRING(prd_key, 7,LEN(prd_key)) AS sls_prd_key, -- Generating derived column  sls_prd_key from prd_key
prd_nm,
prd_cost,
CASE	-- normalizing the value for understandibility.
	WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
	WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other Sales'
	WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
	WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
	ELSE 'n/a'
END AS prd_line,
CAST(prd_start_dt AS DATE) AS prd_start_dt, -- Removing the time part since it was 00:00:000 everywhere
CAST(
	LEAD(prd_start_dt,1)OVER(PARTITION BY prd_key ORDER BY prd_start_dt) - 1  /*Normalizing the inconsistency in the date. 
																				Since one product cant have multiple price in same window */
	AS DATE
	) AS prd_end_dt
from bronze.crm_prd_info


