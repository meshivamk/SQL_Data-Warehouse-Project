/*
Cleaning and Normalizing the data for table silver.erp_cust_az12.
And then using the truncate and insert method for inserting the data.
*/
TRUNCATE TABLE silver.erp_cust_az12
INSERT INTO silver.erp_cust_az12
	(
	cid,
	bdate,
	gen
	)
SELECT 
	CASE  -- The cid here refrences to the cst_id in table bronze.crm_cust_info.  But its doesnt have NAS at start, so we clean that up
		WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(CID)) -- Remove NAS where NAS is present
		ELSE cid
	END AS cid,
	CASE 
		WHEN bdate > GETDATE() THEN NULL -- bdate cannot be greater than currentdate 
		ELSE bdate 
	END AS bdate,
	CASE		-- Normalizing Values
		WHEN UPPER(TRIM(gen)) IN ( 'F', 'FEMALE') THEN 'Female' 
		WHEN UPPER(TRIM(gen)) IN ( 'M', 'MALE') THEN 'Male'
		ELSE 'n/a'
	END AS gen
FROM bronze.erp_cust_az12




