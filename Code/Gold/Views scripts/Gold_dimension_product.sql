/*
	Created View Product-dimension by joining multiple tables.
	This view consists of all the avilable info about product coming from the source.
*/

IF OBJECT_ID('gold.dim_products', 'V') IS NOT NULL
    DROP VIEW gold.dim_products;
GO
CREATE VIEW gold.dim_products AS
SELECT
    ROW_NUMBER() OVER (ORDER BY pn.prd_start_dt, pn.sls_prd_key) AS product_key, -- Surrogate key(check in notes)
    pn.prd_id       AS product_id,
    pn.sls_prd_key      AS product_number,
    pn.prd_nm       AS product_name,
    pn.prd_cat_id       AS category_id,
    pc.cat          AS category,
    pc.subcat       AS subcategory,
    pc.maintenance  AS maintenance,
    pn.prd_cost     AS cost,
    pn.prd_line     AS product_line,
    pn.prd_start_dt AS start_date
FROM silver.crm_prd_info pn
LEFT JOIN silver.erp_px_cat_g1v2 pc
    ON pn.prd_cat_id = pc.id
WHERE pn.prd_end_dt IS NULL; /* This filters out the history for thr products.
                                >> And keeps only the current products  */

