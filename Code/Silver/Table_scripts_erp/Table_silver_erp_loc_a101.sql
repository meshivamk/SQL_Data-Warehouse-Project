/*
Cleaning and Normalizing the data for table silver.erp_loc_a101.
And then using the truncate and insert method for inserting the data.
*/


TRUNCATE TABLE silver.erp_loc_a101
INSERT INTO silver.erp_loc_a101
	(
	cid,
	cntry
	)
SELECT 
	REPLACE(cid, '-', '') AS cid,
	CASE 
		WHEN TRIM(cntry) = 'DE' THEN 'Germany'
		WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
		WHEN TRIM(cntry) = ''  OR cntry IS NULL THEN 'n/a'
		ELSE cntry
	END AS cntry
FROM bronze.erp_loc_a101
