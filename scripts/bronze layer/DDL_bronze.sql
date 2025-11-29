/* 
===========================================================
PURPOSE OF THIS SCRIPT
-----------------------------------------------------------
This script DROPS and RECREATES the Bronze layer tables 
required for the CRM and ERP source systems.

⚠ WARNING:
Running this script will DELETE all existing data in these 
tables because DROP TABLE removes the table completely.

Use this script only when you want to reset the Bronze 
schema and reload all data from the beginning.
===========================================================
*/


IF OBJECT_ID ('Silver.crm_cust_info' , 'U') IS NOT NULL 
    DROP TABLE Silver.crm_cust_info;
CREATE TABLE Silver.crm_cust_info (
    cst_id INT,
    cst_key NVARCHAR(50),
    cst_firstName NVARCHAR(50),
    cst_lastName NVARCHAR(50),
    cst_meterial_status NVARCHAR(50),
    cst_gendr NVARCHAR(10),
    cst_create_date DATE,
	dwh_create_date DAtetime2 default GEtDate()
);

IF OBJECT_ID ('Silver.crm_prd_info' , 'U') IS NOT NULL 
    DROP TABLE Silver.crm_prd_info;
CREATE TABLE Silver.crm_prd_info (
    prd_id INT,
    prd_key NVARCHAR(50),
	cat_id NVARCHAR(50),
    prd_nm NVARCHAR(50),
    prd_cost INT,
    prd_line NVARCHAR(50),
    prd_start_dt Date,
    prd_end_dt DATE,
	dwh_create_date DAtetime2 default GEtDate()
);

IF OBJECT_ID ('Silver.crm_sales_details' , 'U') IS NOT NULL 
    DROP TABLE Silver.crm_sales_details;
CREATE TABLE Silver.crm_sales_details (
    sls_ord_num NVARCHAR(50),
    sls_prd_key NVARCHAR(50),
    sls_cust_id INT,
    sls_order_dt date,
    sls_ship_dt date,
    sls_due_dt date,
    sls_sales INT,
    sls_quantit INT,
    sls_price INT,
	dwh_create_date DAtetime2 default GEtDate()
);

IF OBJECT_ID ('Silver.erp_loc_a101' , 'U') IS NOT NULL 
    DROP TABLE Silver.erp_loc_a101;
CREATE TABLE Silver.erp_loc_a101 (
    cid NVARCHAR(50),
    cntry NVARCHAR(50),
	dwh_create_date DAtetime2 default GEtDate()
);

IF OBJECT_ID ('Silver.erp_cust_az12' , 'U') IS NOT NULL 
    DROP TABLE Silver.erp_cust_az12;
CREATE TABLE Silver.erp_cust_az12 (
    cid NVARCHAR(50),
    bdate DATE,
    gen NVARCHAR(50),
	dwh_create_date DAtetime2 default GEtDate()
);

IF OBJECT_ID ('Silver.erp_px_cat_g1v2' , 'U') IS NOT NULL 
    DROP TABLE Silver.erp_px_cat_g1v2;
CREATE TABLE Silver.erp_px_cat_g1v2 (
    id NVARCHAR(50),
    cat NVARCHAR(50),
    subcat NVARCHAR(50),
    maintenance NVARCHAR(50),
	dwh_create_date DAtetime2 default GEtDate()
);
