/*
===========================================================
Quality checks
===========================================================
Purpose:
This script performs value-quality checks for data consistency, 
accuracy, and standardization across the Silver schema. 
It includes checks for:
- Null or duplicate primary keys
- Unwanted spaces in string fields
- Data standardization and consistency (e.g., gender, marital status, product lines)
- Invalid date ranges and ordering
- Data consistency between related fields (e.g., sales = quantity * price)

Action Performed:
- Using node execution, run this quality check after loading the Silver layer.
- Investigate and receive any description or report of issues detected during these checks.
- Helps identify potential data problems before using the Silver layer for downstream processes.

Tables Checked:
- CRM: crm_cust_info, crm_prd_info, crm_sales_details
- ERP: erp_cust_az12, erp_px_cat_g1v2, erp_loc_a101

Notes:
- This script is read-only; it does not modify table data.
- Ensure Silver layer has been loaded before running this script.

Author: ICE
Date: 2025-11-30
===========================================================
*/


----------------------------------------------------------
                    --- CRM Table ---
----------------------------------------------------------

----------------------------------------------------------
-- CRM: crm_cust_info
----------------------------------------------------------
-- check for nulls or duplicates in PK
-- expectation : No result
SELECT *
FROM (
    SELECT 
        cst_id,
        COUNT(*) AS cnt
    FROM silver.crm_cust_info
    GROUP BY cst_id
) AS t
WHERE t.cst_id IS NULL OR t.cnt > 1;

-- check unwanted spaces 
-- expectation : No result
SELECT cst_firstname
FROM silver.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname);

SELECT cst_key
FROM silver.crm_cust_info
WHERE cst_key != TRIM(cst_key);

-- data standardization & consistency
SELECT DISTINCT cst_gendr
FROM silver.crm_cust_info;

-- data standardization & consistency
SELECT DISTINCT cst_meterial_status
FROM silver.crm_cust_info;

----------------------------------------------------------
-- CRM: crm_prd_info
----------------------------------------------------------
-- check for nulls or duplicates in PK
-- expectation : No result
SELECT *
FROM (
    SELECT 
        prd_id,
        COUNT(*) AS cnt
    FROM silver.crm_prd_info
    GROUP BY prd_id
) AS t
WHERE t.prd_id IS NULL OR t.cnt > 1;

-- check unwanted spaces 
-- expectation : No result
SELECT prd_nm
FROM silver.crm_prd_info
WHERE prd_nm != TRIM(prd_nm);

-- check null or negative numbers
-- expectation : No result
SELECT prd_cost
FROM silver.crm_prd_info
WHERE prd_cost < 0 OR prd_cost IS NULL;

-- data standardization & consistency
SELECT DISTINCT prd_line
FROM silver.crm_prd_info;

-- check for invalid dates
SELECT DISTINCT *
FROM silver.crm_prd_info
WHERE prd_end_dt < prd_start_dt;

----------------------------------------------------------
-- CRM: crm_sales_details
----------------------------------------------------------
-- check for invalid date 
-- we can't check here using silver table because we work in bronze with int but we cast it to date in silver table
SELECT NULLIF(sls_order_dt, 0) AS sls_order_dt
FROM bronze.crm_sales_details
WHERE sls_order_dt <= 0 OR LEN(sls_order_dt) != 8 OR sls_order_dt > 20251010 OR sls_order_dt < 20041225;

SELECT NULLIF(sls_ship_dt, 0) AS sls_ship_dt
FROM bronze.crm_sales_details
WHERE sls_ship_dt <= 0 OR LEN(sls_ship_dt) != 8 OR sls_ship_dt > 20251010 OR sls_ship_dt < 20041225;

SELECT NULLIF(sls_due_dt, 0) AS sls_due_dt
FROM bronze.crm_sales_details
WHERE sls_due_dt <= 0 OR LEN(sls_due_dt) != 8 OR sls_due_dt > 20251010 OR sls_due_dt < 20041225;

-- check for invalid date order
SELECT *
FROM silver.crm_sales_details
WHERE sls_order_dt > sls_ship_dt OR sls_order_dt > sls_due_dt;

-- check data consistency between sales, quantity, and price
-- Sales = quantity * Price 
-- values must not be null, zero, or negative
SELECT DISTINCT
    sls_sales,
    sls_quantit,
    sls_price
FROM silver.crm_sales_details
ORDER BY sls_sales, sls_quantit, sls_price;

----------------------------------------------------------
              --- ERP Table ---
----------------------------------------------------------

----------------------------------------------------------
-- ERP: erp_px_cat_g1v2
----------------------------------------------------------
-- unwanted spaces     
SELECT 
    id,
    cat,
    subcat,
    maintenance
FROM silver.erp_px_cat_g1v2 
WHERE cat != TRIM(cat);

SELECT 
    id,
    cat,
    subcat,
    maintenance
FROM bronze.erp_px_cat_g1v2 
WHERE subcat != TRIM(subcat);

SELECT 
    id,
    cat,
    subcat,
    maintenance
FROM silver.erp_px_cat_g1v2 
WHERE maintenance != TRIM(maintenance);

-- data standardization & consistency
SELECT DISTINCT maintenance
FROM silver.erp_px_cat_g1v2;

SELECT DISTINCT cat
FROM silver.erp_px_cat_g1v2;

SELECT DISTINCT subcat
FROM silver.erp_px_cat_g1v2;

----------------------------------------------------------
-- ERP: erp_cust_az12
----------------------------------------------------------
-- check from key 
SELECT cid
FROM silver.erp_cust_az12;

-- identify out-of-range dates 
SELECT bdate
FROM silver.erp_cust_az12;

-- data standardization & consistency
SELECT DISTINCT gen
FROM silver.erp_cust_az12;

----------------------------------------------------------
-- ERP: erp_loc_a101
----------------------------------------------------------
-- fix unwanted characters
SELECT cid
FROM silver.erp_loc_a101;

-- data standardization & consistency
SELECT DISTINCT cntry
FROM silver.erp_loc_a101
ORDER BY cntry;
