IF OBJECT_ID('gold.fact_sales', 'V') IS NOT NULL
    DROP VIEW gold.fact_sales;
GO

CREATE VIEW gold.fact_sales AS
SELECT
    sd.sls_ord_num  AS order_number,
    pr.product_key  AS product_key,     -- Used the surrogate keys instead of the ID's to easily connect facts with dimension
    cu.customer_key AS customer_key,    -- Used the surrogate keys instead of the ID's to easily connect facts with dimension
    sd.sls_order_dt AS order_date,
    sd.sls_ship_dt  AS shipping_date,
    sd.sls_due_dt   AS due_date,
    sd.sls_sales    AS sales_amount,
    sd.sls_quantity AS quantity,
    sd.sls_price    AS price
FROM silver.crm_sales_details sd
LEFT JOIN gold.dim_products pr          -- Joined with view 
    ON sd.sls_prd_key = pr.product_number
LEFT JOIN gold.dim_customers cu         -- Joined with view 
    ON sd.sls_cust_id = cu.customer_id;
