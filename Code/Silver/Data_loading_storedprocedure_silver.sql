/* 
Created a stored procedure for inserting bulk data into tables from the source bronze schema crm and erp tables,
this highlights the loading process of the Silver layer
It performs the following actions:
    - Truncates the silver tables before loading data.
	- Then we clean and normalize the data (All the anomalies found in Data quality checks.)
    - Then we insert data into the silver tables using insert - Select Method
*/



CREATE OR ALTER PROCEDURE silver.load_data -- Created Stored procedure
AS 
	BEGIN
		DECLARE @starttime DATETIME, @Endtime DATETIME, @batchstarttime DATETIME, @batchendtime DATETIME; -- Variable for time completion
		BEGIN TRY
		print '============================================================';
		print 'INSERTION IN SILVER LAYER STARTS';
	-- Insertion into silver.crm_cust_info
	set @batchstarttime = getdate() -- Set Batch start Starttime 
		set @starttime = getdate() -- Set Individual Table starttime (Can be done for Each Table) 
		TRUNCATE TABLE silver.crm_cust_info;
		print '------------------------------------------------------------';
		print 'Inserting data into silver.crm_cust_info, Insertion Started' ;
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
		set @Endtime = getdate() --Set Individual Table Endtime
		print char(13) + 'Insertion into silver.crm_cust_info completed' + char(13);
		print 'Time taken for insertion is ' + cast(datediff(second, @starttime, @endtime) as varchar) + ' seconds';
		print '------------------------------------------------------------' + char(13);


		-- Insertion into silver.crm_prd_info
		TRUNCATE TABLE silver.crm_prd_info
		print '------------------------------------------------------------';
		print 'Inserting data into silver.crm_prd_info, Insertion Started';
		set @starttime = getdate()
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
		set @Endtime = getdate()
		print char(13) + 'Insertion into silver.crm_prd_info completed'+ char(13);
		print 'Time taken for insertion is ' + cast(datediff(second, @starttime, @endtime) as varchar) + ' seconds';
		print '------------------------------------------------------------' + char(13);



		-- Insertion into silver.crm_sales_details

		TRUNCATE TABLE silver.crm_sales_details; 
		print '------------------------------------------------------------';
		print 'Inserting data into silver.crm_sales_details, Insertion Started';
		set @starttime = getdate()
		INSERT INTO silver.crm_sales_details	
			(
			sls_ord_num,
			sls_prd_key,
			sls_cust_id,
			sls_order_dt,
			sls_ship_dt,
			sls_due_dt,
			sls_sales,
			sls_quantity,
			sls_price
			)
		SELECT 
			sls_ord_num,
			sls_prd_key,
			sls_cust_id,
			sls_order_dt,
			sls_ship_dt,
			sls_due_dt,
			CASE  -- Converting sales value onto valid sales values using sls_quantity * sls_price
				WHEN sls_sales != sls_quantity * sls_price THEN sls_quantity * sls_price
				ELSE sls_sales
			END as sls_sales,
			sls_quantity,
			sls_price
		FROM
			(
			SELECT
				sls_ord_num,
				sls_prd_key,
				sls_cust_id,
				CASE		-- -- Handle invalid values of date, if length of date is less then or greater than 8 and is out of range
					WHEN LEN(sls_order_dt) = 8 -- To check valid dates
					AND (ABS(sls_order_dt)) > 19900101 -- Date should be greater then start date of business
					AND (ABS(sls_order_dt)) < CAST(REPLACE(CAST(CAST(GETDATE() AS DATE) AS VARCHAR), '-', '') AS INT) -- Date should be less than current date
					THEN CAST(CAST(ABS(sls_order_dt) AS VARCHAR) AS DATE) -- First convert into varchar then date, since direct conversion not allowed
					ELSE NULL
				END AS sls_order_dt, 
				CASE			-- Same as sls_order_dt
					WHEN LEN(sls_ship_dt) = 8 AND (ABS(sls_ship_dt)) > 19900101 AND ABS(sls_ship_dt) < CAST(REPLACE(CAST(CAST(GETDATE() AS DATE) AS VARCHAR), '-', '') AS INT) THEN CAST(CAST(ABS(sls_ship_dt) AS VARCHAR) AS DATE)
					ELSE NULL
				END AS sls_ship_dt,
				CASE			-- Same as sls_order_dt
					WHEN LEN(ABS(sls_due_dt)) = 8 AND ABS(sls_due_dt) > 19900101 AND ABS(sls_due_dt) < CAST(REPLACE(CAST(CAST(GETDATE() AS DATE) AS VARCHAR), '-', '') AS INT) THEN CAST(CAST(ABS(sls_due_dt) AS VARCHAR) AS DATE)
					ELSE NULL
				END AS sls_due_dt,
				sls_sales,
				sls_quantity,
				CASE
					WHEN sls_price IS NULL THEN ABS(sls_sales)/NULLIF(sls_quantity, 0) -- Convert null prices into real prices using sls_sales and sls_quantity
					WHEN sls_price < 0 THEN ABS(sls_price) -- Converting negative price to positive
					ELSE sls_price
				END AS sls_price
			FROM bronze.crm_sales_details
		)T
		set @Endtime = getdate()
		print char(13) + 'Insertion into silver.crm_sales_details completed'+ char(13);
		print 'Time taken for insertion is ' + cast(datediff(second, @starttime, @endtime) as varchar) + ' seconds';
		print '------------------------------------------------------------' + char(13);


		-- Insertion into silver.erp_loc_a101
		TRUNCATE TABLE silver.erp_loc_a101
		print '------------------------------------------------------------';
		print 'Inserting data into silver.erp_loc_a101, Insertion Started' ;
		set @starttime = getdate()
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
		print char(13) + 'Insertion into silver.erp_loc_a101 completed' + char(13);
		print 'Time taken for insertion is ' + cast(datediff(second, @starttime, @endtime) as varchar) + ' seconds';
		print '------------------------------------------------------------' + char(13);


		-- Insertion into silver.erp_cust_az12
		TRUNCATE TABLE silver.erp_cust_az12
		print '------------------------------------------------------------';
		print 'Inserting data into silver.erp_cust_az12, Insertion Started' ;
		set @starttime = getdate()
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
		set @Endtime = getdate()
		print char(13) + 'Insertion into silver.erp_cust_az12 completed'+ char(13);
		print 'Time taken for insertion is ' + cast(datediff(second, @starttime, @endtime) as varchar) + ' seconds';
		print '------------------------------------------------------------' + char(13);


		-- Insertion into silver.erp_px_cat_g1v2
		TRUNCATE TABLE silver.erp_px_cat_g1v2
		print '------------------------------------------------------------';
		print 'Inserting data into silver.erp_px_cat_g1v2, Insertion Started' ;
		set @starttime = getdate()
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
		set @Endtime = getdate()
	set @batchendtime = getdate() -- Set Batch Endtime
		print char(13) + 'Insertion into silver.erp_px_cat_g1v2 completed' +char(13);
		print 'Time taken for insertion is ' + cast(datediff(second, @starttime, @endtime) as varchar) + ' seconds'+ char(13);
		print '------------------------------------------------------------';
		print 'INSERTION IN SILVER LAYER COMPLETED';
		print 'Total time taken is ' + cast(datediff(second, @batchstarttime, @batchendtime) as nvarchar) + ' seconds';
		print '================================================================' + char(13);

	END TRY

	BEGIN CATCH
		print '=========================================='
		print 'ERROR OCCURED DURING LOADING BRONZE LAYER'
		print 'Error Message' + ERROR_MESSAGE();
		print 'Error Message' + cast (ERROR_NUMBER() as nvarchar);
		print 'Error Message' + cast (ERROR_STATE() as nvarchar);
		print '=========================================='
	END CATCH
END
;
