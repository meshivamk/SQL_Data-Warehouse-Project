-- Script For Data Quality Checks 

/* 
	1. Quality checks for null values and duplicate values in the column you want to make primary key.
		Expectations -- no results.
		CHECK AGAIN AFER INSERTING INTO THE SILVER LAYER FOR DATA QUALITY CHECK, FOR SILVER LAYER TABLE.
*/

SELECT 
cst_id,
COUNT(*) --Make sure you dont use column name here else the nulls will be ignored
FROM bronze.crm_cust_info
GROUP BY cst_id
HAVING COUNT(cst_id)>1 or cst_id is null --Make sure to include 'cst_id is null' else you wont get the null values

/* 
	2. To check if theres any empty space in the string
		Expectations -- no results
		CHECK AGAIN AFER INSERTING INTO THE SILVER LAYER FOR DATA QUALITY CHECK, FOR SILVER LAYER TABLE
*/

SELECT
cst_firstname
FROM bronze.crm_cust_info
WHERE LEN(cst_firstname) != LEN(TRIM(cst_firstname)) -- To Check if the name contains space inside it, do for last name too


/* 
	3. To check the possible Values
		Normalize them into a clearly understandable value.
		CHECK AGAIN AFER INSERTING INTO THE SILVER LAYER FOR DATA QUALITY CHECK, FOR SILVER LAYER TABLE
*/

SELECT DISTINCT 
cst_gndr
FROM bronze.crm_cust_info

/* 
	3. To check the possible Values
		Normalize them into a clearly understandable value.
		CHECK AGAIN AFER INSERTING INTO THE SILVER LAYER FOR DATA QUALITY CHECK, FOR SILVER LAYER TABLE
*/

SELECT DISTINCT 
cst_marital_status
FROM bronze.crm_cust_info -- The check can be performed for country, category, subcategory or wherever it allows limited values.



/* 
	4. To check the inconsistency in price, sales and quantity
		Converting them into values that follow business logic or has a logical binding like sales = price * quantity
		Expectations -- no results
		CHECK AGAIN AFER INSERTING INTO THE SILVER LAYER FOR DATA QUALITY CHECK, FOR SILVER LAYER TABLE
*/

SELECT
*
FROM bronze.crm_sales_details
WHERE sls_price <= 0  
	  OR sls_sales < sls_price  
	  OR sls_sales <= 0 
	  OR sls_quantity <= 0 
	  OR (sls_sales != sls_price*sls_quantity) 
	  OR sls_quantity IS NULL  
	  OR sls_price IS NULL;



/* 
	5. To check the invalid dates.
		Converting them into sandard date value and format that is accepted.
		Expectations -- no results
		CHECK AGAIN AFER INSERTING INTO THE SILVER LAYER FOR DATA QUALITY CHECK, FOR SILVER LAYER TABLE
*/

SELECT
*
FROM bronze.crm_sales_details
WHERE LEN(sls_order_dt) != 8 OR sls_order_dt = 0



/* 
	5. To check the invalid dates values of columns like order_dt as order_dt cannot be after ship_dt or due_dt
		Expectations -- no results
		Convert them into some valis values or discard the orders whatever the businees follows.
		CHECK AGAIN AFER INSERTING INTO THE SILVER LAYER FOR DATA QUALITY CHECK, FOR SILVER LAYER TABLE
*/

SELECT
*
FROM bronze.crm_sales_details
WHERE sls_order_dt > sls_ship_dt OR sls_order_dt > sls_due_dt
