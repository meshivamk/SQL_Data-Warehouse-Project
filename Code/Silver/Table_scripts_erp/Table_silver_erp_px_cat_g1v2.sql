/*
Everything inside the table is already clean and in order in table Bronze.erp_px_cat_g1v2.
So just Truncate and insert the data into silver.erp_px_cat_g1v2.
*/

TRUNCATE TABLE silver.erp_px_cat_g1v2
INSERT INTO silver.erp_px_cat_g1v2
	(
	id,
	cat,
	subcat,
	maintenance
	)
SELECT 
	id,
	cat,
	subcat,
	maintenance
FROM bronze.erp_px_cat_g1v2
