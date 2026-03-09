/* 
DDL SCRIPTS:- CREATE GOLD VIEWS*/

CREATE VIEW GOLD.DIM_CUSTOMERS AS
SELECT
ROW_NUMBER() OVER (ORDER BY cst_id) AS customer_key,
	ci.cst_id AS customer_id,
	ci.cst_key AS customer_number,
	ci.cst_firstname AS first_name,
	ci.cst_lastname AS last_name,
	la.cntry AS country,
	ci.cst_marital_status AS marital_status,
	CASE WHEN ci.cst_gndr !='/na' THEN ci.cst_gndr
	     ELSE COALESCE(ca.gen,'n/a')
	END AS gender,
	ca.bdate AS birthdate,
	ci.cst_create_date AS create_date
FROM Silver.crm_cust_info ci
LEFT JOIN Silver.erp_cust_az12 ca
ON        ci.cst_key = ca.cid
LEFT JOIN Silver.erp_loc_a101 la
ON        ci.cst_key = la.cid


SELECT DISTINCT
	ci.cst_gndr,
	ca.gen,
	CASE WHEN ci.cst_gndr !='/na' THEN ci.cst_gndr
	     ELSE COALESCE(ca.gen,'n/a')
	END AS new_gen
FROM Silver.crm_cust_info ci
LEFT JOIN Silver.erp_cust_az12 ca
ON        ci.cst_key = ca.cid
LEFT JOIN Silver.erp_loc_a101 la
ON        ci.cst_key = la.cid


CREATE VIEW GOLD.DIM_PRODUCTS AS 
SELECT 
ROW_NUMBER() OVER (ORDER BY pn.prd_start_dt,pn.prd_key) AS product_key,
  pn.prd_id AS product_id,
  pn.prd_key AS product_number,
  pn.prd_nm AS product_name,
  pn.cat_id AS category_id,
  pc.cat AS category,
  pc.subcat AS subcategory,
  pc.maintenance,
  pn.prd_cost AS cost,
  pn.prd_line AS product_line,
  pn.prd_start_dt AS start_date
  FROM Silver.crm_prd_info pn
  LEFT JOIN Silver.erp_px_cat_g1v2 pc
  ON pn.cat_id = pc.id
  WHERE prd_end_dt IS NULL 
 
 CREATE VIEW GOLD.FACT_SALES AS
 SELECT
 sd.sls_ord_num AS order_number,
 cu.customer_key,
 sd.sls_order_dt AS order_date,
 sd.sls_ship_dt AS shipping_date,
 sd.sls_due_dt AS due_date,
 sd.sls_sales AS sales_amount,
 sd.sls_quantity AS quantity,
 sd.sls_price AS price
 FROM Silver.crm_sales_details sd
 LEFT JOIN Gold.DIM_PRODUCTS pr
 ON sd.sls_prd_key = pr.product_name
 LEFT JOIN Gold.DIM_CUSTOMERS cu
 ON sd.sls_cust_id = cu.customer_id
