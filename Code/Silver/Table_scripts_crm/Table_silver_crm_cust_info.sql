/*
Cleaning and Normalizing the data for table silver.crm_cust_info.
And then using the truncate and insert method for inserting the data.
*/

TRUNCATE TABLE silver.crm_cust_info;
INSERT INTO silver.crm_cust_info
      (
       cst_id
      ,cst_key
      ,cst_firstname
      ,cst_lastname
      ,cst_marital_status
      ,cst_gndr
      ,cst_create_date
	  )
SELECT 
	cst_id,
	cst_key,
	TRIM(cst_firstname) AS cst_firstname, -- Remove starting and trailing Empty Spaces
	TRIM(cst_lastname) AS cst_lastname,	  -- Remove starting and trailing Empty Spaces
	CASE  --Normalized for better Readibility
		WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married' 
		WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
		ELSE 'n/a' 
	END AS cst_marital_status,
	CASE   --Normalized for better Readibility
		WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
		WHEN UPPER(TRIM(cst_gndr)) = 'F'  THEN 'Female'
		ELSE 'n/a'
	END AS  cst_gndr,
	cst_create_date
FROM
(
	SELECT 
	*   -- Selecting only one value for each cst_id that has been created most recently
	FROM
	(
		SELECT 
			*,
			ROW_NUMBER()OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) AS RN 
		FROM bronze.crm_cust_info
		WHERE cst_id IS NOT NULL
	)T -- Always handle nulls before using ROW_NUMBER, so that you dont rank even the null values
	WHERE RN = 1
)X
