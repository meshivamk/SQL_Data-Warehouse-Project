/* 
Created a stored procedure for inserting bulk data into tables from the source csv files,
this highlights the loading process of the Bronze layer
It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Uses the `BULK INSERT` command to load data from csv Files to bronze tables.
*/

create or alter procedure bronze.load_data -- Created Stored procedure
As 
Begin
	declare @starttime datetime, @Endtime datetime, @batchstarttime datetime, @batchendtime datetime; -- Variable for time completion
	Begin try
		print '============================================================';
		print 'INSERTION STARTS';
	set @batchstarttime = getdate() -- Set Batch start Starttime 
		set @starttime = getdate() -- Set Individual Table starttime (Can be done for Each Table) 
		truncate table bronze.crm_cust_info;
		print '------------------------------------------------------------';
		print 'Inserting data into bronze.crm_cust_info, Insertion Started' ;
		bulk insert bronze.crm_cust_info
		from 'C:\Users\Lenovo\Downloads\SQL\SQL Project\sql-data-warehouse-project- Baara\datasets\source_crm\cust_info.csv'
			with(
			firstrow =2, --Since first row in the data is attribute
			fieldterminator = ',',  -- How the vlues are seperated
			tablock
			);
		set @Endtime = getdate() --Set Individual Table Endtime
		print char(13) + 'Insertion into bronze.crm_cust_info completed' + char(13);
		print 'Time taken for insertion is ' + cast(datediff(second, @starttime, @endtime) as varchar) + ' seconds';
		print '------------------------------------------------------------' + char(13);


		truncate table bronze.crm_prd_info;
		print '------------------------------------------------------------';
		print 'Inserting data into bronze.crm_prd_info, Insertion Started';
		bulk insert bronze.crm_prd_info
		from 'C:\Users\Lenovo\Downloads\SQL\SQL Project\sql-data-warehouse-project- Baara\datasets\source_crm\prd_info.csv'
			with(
			firstrow =2,
			fieldterminator = ',',
			tablock
			);
		print char(13) + 'Insertion into bronze.crm_prd_info completed';
		print '------------------------------------------------------------' + char(13);

		truncate table bronze.crm_sales_details;
		print '------------------------------------------------------------';
		print 'Inserting data into bronze.crm_sales_details, Insertion Started';
		bulk insert bronze.crm_sales_details
		from 'C:\Users\Lenovo\Downloads\SQL\SQL Project\sql-data-warehouse-project- Baara\datasets\source_crm\sales_details.csv'
			with(
			firstrow =2,
			fieldterminator = ',',
			tablock
			);
		print char(13) + 'Insertion into bronze.crm_sales_details completed';
		print '------------------------------------------------------------' + char(13);


		truncate table bronze.erp_loc_a101;
		print '------------------------------------------------------------';
		print 'Inserting data into bronze.erp_loc_a101, Insertion Started' ;
		bulk insert bronze.erp_loc_a101
		from 'C:\Users\Lenovo\Downloads\SQL\SQL Project\sql-data-warehouse-project- Baara\datasets\source_erp\LOC_A101.csv'
			with(
			firstrow =2,
			fieldterminator = ',',
			tablock
			);
		print char(13) + 'Insertion into bronze.erp_loc_a101 completed';
		print '------------------------------------------------------------' + char(13);


		truncate table bronze.erp_cust_az12;
		print '------------------------------------------------------------';
		print 'Inserting data into bronze.erp_cust_az12, Insertion Started' ;
		bulk insert bronze.erp_cust_az12
		from 'C:\Users\Lenovo\Downloads\SQL\SQL Project\sql-data-warehouse-project- Baara\datasets\source_erp\CUST_AZ12.csv'
			with(
			firstrow =2,
			fieldterminator = ',',
			tablock
			);
		print char(13) + 'Insertion into bronze.erp_cust_az12 completed';
		print '------------------------------------------------------------' + char(13);



		truncate table bronze.erp_px_cat_g1v2;
		print '------------------------------------------------------------';
		print 'Inserting data into bronze.erp_px_cat_g1v2, Insertion Started' ;
		bulk insert bronze.erp_px_cat_g1v2
		from 'C:\Users\Lenovo\Downloads\SQL\SQL Project\sql-data-warehouse-project- Baara\datasets\source_erp\PX_CAT_G1V2.csv'
			with(
			firstrow =2,
			fieldterminator = ',',
			tablock
			);
		print char(13) + 'Insertion into bronze.erp_px_cat_g1v2 completed' +char(13);
	set @batchendtime = getdate() -- Set Batch Endtime
		print 'INSERTION COMPLETED';
		print 'Total time taken is ' + cast(datediff(second, @batchstarttime, @batchendtime) as nvarchar) + ' seconds';
		print '================================================================' + char(13);
	end try

	begin catch
		print '=========================================='
		print 'ERROR OCCURED DURING LOADING BRONZE LAYER'
		print 'Error Message' + ERROR_MESSAGE();
		print 'Error Message' + cast (ERROR_NUMBER() as nvarchar);
		print 'Error Message' + cast (ERROR_STATE() as nvarchar);
		print '=========================================='
	end catch
end
;

