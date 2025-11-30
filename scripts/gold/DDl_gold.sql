/*
===========================================================
Gold Layer Views
===========================================================
Purpose (hidden):
-- This script creates the Gold layer views for the Data Warehouse.
-- It includes dimension tables for products and customers, 
-- and a fact table for sales, based on the Silver layer.
-- Friendly comments are added to guide developers and analysts.
===========================================================
*/

----------------------------------------------------------
-- GOLD DIMENSION: Products
----------------------------------------------------------
-- This view creates a product dimension, including product details,
-- category info, and maintenance status. Historical products are filtered out.
CREATE VIEW gold.dim_products AS
(
    SELECT 
        ROW_NUMBER() OVER (ORDER BY pn.prd_start_dt, pn.prd_key) AS product_key,
        pn.prd_id AS product_id,
        pn.prd_key AS product_number,
        pn.prd_nm AS product_name,
        pn.cat_id AS category_id,
        pc.cat AS category,
        pc.subcat AS sub_category,
        pn.prd_cost AS product_cost,
        pn.prd_line AS product_line,
        pc.maintenance,
        pn.prd_start_dt AS start_date
    FROM silver.crm_prd_info pn
    LEFT JOIN silver.erp_px_cat_g1v2 pc
        ON pn.cat_id = pc.id
    WHERE prd_end_dt IS NULL -- only current products
);

----------------------------------------------------------
-- GOLD DIMENSION: Customers
----------------------------------------------------------
-- This view creates a customer dimension including personal info,
-- country, gender, marital status, and birth date.
CREATE VIEW gold.dim_customers AS
(
    SELECT 
        ROW_NUMBER() OVER (ORDER BY cst_id) AS customer_key,
        Ci.cst_id AS customer_id,
        Ci.cst_key AS customer_number,
        Ci.cst_firstName AS first_name,
        Ci.cst_lastName AS last_name, 
        Cl.cntry AS country,
        CASE 
            WHEN Ci.cst_gendr != 'n/a' THEN Ci.cst_gendr 
            ELSE COALESCE(Ca.gen, 'n/a')
        END AS gender,
        Ci.cst_meterial_status AS meterial_status,
        Ca.bdate AS birth_date,
        Ci.cst_create_date AS create_date
    FROM silver.crm_cust_info AS Ci
    LEFT JOIN silver.erp_cust_az12 AS Ca 
        ON Ci.cst_key = Ca.cid
    LEFT JOIN silver.erp_loc_a101 AS Cl
        ON Ci.cst_key = Cl.cid
);

----------------------------------------------------------
-- GOLD FACT TABLE: Sales
----------------------------------------------------------
-- This view creates the sales fact table, linking to product and customer dimensions.
-- Includes order, shipping, and due dates, as well as sales amount, quantity, and price.
CREATE VIEW gold.fact_sales AS 
(
    SELECT 
        sd.sls_ord_num AS order_number,
        pr.product_key,
        cu.customer_key,
        sd.sls_order_dt AS order_date,
        sd.sls_ship_dt AS shipping_date,
        sd.sls_due_dt AS during_date,
        sd.sls_sales AS sales,
        sd.sls_quantit AS quantity, 
        sd.sls_price AS price
    FROM silver.crm_sales_details sd
    LEFT JOIN gold.dim_products pr
        ON pr.product_number = sd.sls_prd_key
    LEFT JOIN gold.dim_customers cu
        ON sd.sls_cust_id = cu.customer_id
);
