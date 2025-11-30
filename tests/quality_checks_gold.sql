/*
===========================================================
Quality Checks - Gold Layer
===========================================================
Purpose:
This script performs data quality validation on the Gold layer.
It checks:
- Primary key uniqueness for products and customers
- Duplicate customer records
- Data integration consistency (gender from CRM and ERP)
===========================================================
*/


----------------------------------------------------------
-- 1️⃣ Product Key Uniqueness
-- Checks if there are duplicate product keys in active products
----------------------------------------------------------
SELECT prd_key, COUNT(*) AS duplicate_count
FROM (
    SELECT 
        pn.prd_id,
        pn.prd_key,
        pn.prd_nm,
        pn.cat_id,
        pc.cat,
        pc.subcat,
        pn.prd_cost,
        pn.prd_line,
        pc.maintenance,
        pn.prd_start_dt,
        pn.prd_end_dt
    FROM silver.crm_prd_info pn
    LEFT JOIN silver.erp_px_cat_g1v2 pc
        ON pn.cat_id = pc.id
    WHERE prd_end_dt IS NULL
) t
GROUP BY prd_key
HAVING COUNT(*) > 1;

----------------------------------------------------------
-- 2️⃣ Customer Record Uniqueness
-- Checks if a customer ID exists more than once
----------------------------------------------------------
SELECT cst_id, COUNT(*) AS duplicate_count
FROM (
    SELECT 
        Ci.cst_id,
        Ci.cst_key,
        Ci.cst_firstName,
        Ci.cst_lastName, 
        Ci.cst_meterial_status,
        Ci.cst_gendr,
        Ci.cst_create_date,
        Ca.bdate,
        Ca.gen,
        Cl.cntry
    FROM silver.crm_cust_info AS Ci
    LEFT JOIN silver.erp_cust_az12 AS Ca 
        ON Ci.cst_key = Ca.cid
    LEFT JOIN silver.erp_loc_a101 AS Cl
        ON Ci.cst_key = Cl.cid
) t
GROUP BY cst_id
HAVING COUNT(*) > 1;

----------------------------------------------------------
-- 3️⃣ Data Integration: Gender
-- CRM gender is primary; fallback to ERP if missing
----------------------------------------------------------
SELECT DISTINCT	
    Ci.cst_gendr AS crm_gender,
    Ca.gen AS erp_gender,
    CASE 
        WHEN Ci.cst_gendr != 'n/a' THEN Ci.cst_gendr
        ELSE COALESCE(Ca.gen,'n/a')
    END AS integrated_gender
FROM silver.crm_cust_info AS Ci
LEFT JOIN silver.erp_cust_az12 AS Ca 
    ON Ci.cst_key = Ca.cid
LEFT JOIN silver.erp_loc_a101 AS Cl
    ON Ci.cst_key = Cl.cid
ORDER BY crm_gender, erp_gender;
