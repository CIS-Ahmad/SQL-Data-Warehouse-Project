/*====================================================================
 Script Name : bronze.load_bronze
 Layer       : Bronze Layer (Landing Zone)
 Purpose     : 
     This stored procedure loads raw data from CSV files into the
     Bronze Layer tables. It performs the following steps:

         1. Truncates (DELETES ALL DATA) from Bronze tables.
         2. Loads new data using BULK INSERT from CRM and ERP sources.
         3. Measures load duration for each table.
         4. Prints execution logs for monitoring.

 WARNING:
     ⚠️ THIS PROCEDURE TRUNCATES ALL BRONZE TABLES.
     ⚠️ RUNNING THIS WILL DELETE ALL EXISTING DATA IN THE BRONZE LAYER.
     ⚠️ USE WITH CAUTION, ESPECIALLY IN SHARED OR PRODUCTION ENVIRONMENTS.

 Notes:
     - Intended for initial loads and full refresh scenarios.
     - CSV file paths must be valid on the SQL Server host machine.
     - Requires BULK INSERT permissions.

 Author      : ICE
 Version     : 1.0
====================================================================*/

CREATE OR ALTER PROCEDURE bronze.load_bronze
AS
BEGIN

    DECLARE 
        @start_time DATETIME, 
        @end_time DATETIME,
        @start_time_for_bronze_layer DATETIME,
        @end_time_for_bronze_layer DATETIME;

    BEGIN TRY

        SET @start_time_for_bronze_layer = GETDATE();

        PRINT '==================================';
        PRINT '        Loading Bronze Layer';
        PRINT '==================================';

        PRINT '-----------------------------------';
        PRINT '        Loading CRM Tables';
        PRINT '-----------------------------------';

        ----------------------------------------------------------
        -- CRM: cust_info
        ----------------------------------------------------------
        SET @start_time = GETDATE();

        PRINT 'Truncate Table : bronze.crm_cust_info';
        TRUNCATE TABLE bronze.crm_cust_info;

        PRINT 'Bulk Insert : bronze.crm_cust_info';

        -- TODO: put the full file path for this file: cust_info.csv
        BULK INSERT bronze.crm_cust_info
        FROM 'cust_info.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );

        SET @end_time = GETDATE();
        PRINT 'Load Duration : ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '--------------------';


        ----------------------------------------------------------
        -- CRM: prd_info
        ----------------------------------------------------------
        SET @start_time = GETDATE();

        PRINT 'Truncate Table : bronze.crm_prd_info';
        TRUNCATE TABLE bronze.crm_prd_info;

        PRINT 'Bulk Insert : bronze.crm_prd_info';

        -- TODO: put the full file path for this file: prd_info.csv
        BULK INSERT bronze.crm_prd_info
        FROM 'prd_info.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );

        SET @end_time = GETDATE();
        PRINT 'Load Duration : ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '--------------------';


        ----------------------------------------------------------
        -- CRM: sales_details
        ----------------------------------------------------------
        SET @start_time = GETDATE();

        PRINT 'Truncate Table : bronze.crm_sales_details';
        TRUNCATE TABLE bronze.crm_sales_details;

        PRINT 'Bulk Insert : bronze.crm_sales_details';

        -- TODO: put the full file path for this file: sales_details.csv
        BULK INSERT bronze.crm_sales_details
        FROM 'sales_details.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );

        SET @end_time = GETDATE();
        PRINT 'Load Duration : ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '--------------------';


        PRINT '-----------------------------------';
        PRINT '        Loading ERP Tables';
        PRINT '-----------------------------------';

        ----------------------------------------------------------
        -- ERP: cust_az12
        ----------------------------------------------------------
        SET @start_time = GETDATE();

        PRINT 'Truncate Table : bronze.erp_cust_az12';
        TRUNCATE TABLE bronze.erp_cust_az12;

        PRINT 'Bulk Insert : bronze.erp_cust_az12';

        -- TODO: put the full file path for this file: CUST_AZ12.csv
        BULK INSERT bronze.erp_cust_az12
        FROM 'CUST_AZ12.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );

        SET @end_time = GETDATE();
        PRINT 'Load Duration : ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '--------------------';


        ----------------------------------------------------------
        -- ERP: loc_a101
        ----------------------------------------------------------
        SET @start_time = GETDATE();

        PRINT 'Truncate Table : bronze.erp_loc_a101';
        TRUNCATE TABLE bronze.erp_loc_a101;

        PRINT 'Bulk Insert : bronze.erp_loc_a101';

        -- TODO: put the full file path for this file: LOC_A101.csv
        BULK INSERT bronze.erp_loc_a101
        FROM 'LOC_A101.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );

        SET @end_time = GETDATE();
        PRINT 'Load Duration : ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '--------------------';


        ----------------------------------------------------------
        -- ERP: px_cat_g1v2
        ----------------------------------------------------------
        SET @start_time = GETDATE();

        PRINT 'Truncate Table : bronze.erp_px_cat_g1v2';
        TRUNCATE TABLE bronze.erp_px_cat_g1v2;

        PRINT 'Bulk Insert : bronze.erp_px_cat_g1v2';

        -- TODO: put the full file path for this file: PX_CAT_G1V2.csv
        BULK INSERT bronze.erp_px_cat_g1v2
        FROM 'PX_CAT_G1V2.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );

        SET @end_time = GETDATE();
        PRINT 'Load Duration : ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '--------------------';


        SET @end_time_for_bronze_layer = GETDATE();

        PRINT '=============================================';
        PRINT 'Loading bronze layer is completed ...';
        PRINT 'Load Duration for bronze layer : ' 
              + CAST(DATEDIFF(SECOND, @start_time_for_bronze_layer, @end_time_for_bronze_layer) AS NVARCHAR)
        PRINT '=============================================';

    END TRY

    BEGIN CATCH
        PRINT '======================================';
        PRINT 'Error Occurred when loading Bronze layer';
        PRINT 'Error Message : ' + ERROR_MESSAGE();
        PRINT 'Error Number  : ' + CAST(ERROR_NUMBER() AS NVARCHAR);
        PRINT '======================================';
    END CATCH;

END;
GO
