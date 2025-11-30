
/* 
===============================================================
   Stored Procedure: Load Silver Layer (Bronze -> Silver)
===============================================================

PURPOSE:
    This stored procedure performs the ETL (Extract, Transform, Load)
    process used to populate the **Silver schema** tables 
    from the **Bronze schema**.

ACTION PERFORMED:
    - Truncate Silver tables.
    - Transform and clean data.
    - Insert processed data from Bronze tables into Silver tables.

⚠ WARNING:
    Running this procedure will REMOVE all existing data inside the
    Silver tables because TRUNCATE TABLE is used before inserting new data.
    Execute this procedure only when you intend to refresh or rebuild the
    Silver layer from scratch.

===============================================================
*/


create or alter procedure silver.load_silver as

begin
declare 
	@start_Load_time DATETIME,
	@end_load_time DATETIME,
	@end_All_load_time DATETIME,
	@start_All_Load_time DATETIME;

	begin try
		
				set @start_All_Load_time = GETDATE();
				PRINT '==================================';
				PRINT '        Loading Silver Layer';
				PRINT '==================================';

				PRINT '-----------------------------------';
				PRINT '        Loading CRM Tables';
				PRINT '-----------------------------------';

				----------------------------------------------------------
				-- CRM: cust_info
				----------------------------------------------------------
		
				set @start_Load_time = GETDATE();

			print 'truncate table : silver.crm_cust_info';
			truncate table silver.crm_cust_info;
			print 'insert into table :silver.crm_cust_info';
			insert into silver.crm_cust_info( 
				cst_id,
				cst_key,
				cst_firstName,
				cst_lastName,
				cst_meterial_status,
				cst_gendr,
				cst_create_date
			   )

			SELECT cst_id,
				   cst_key
				  ,trim(cst_firstName) as cst_firstName
				  ,trim(cst_lastName) as cst_lastName
				  ,
				   case when Upper (trim(cst_meterial_status)) ='S' then 'Singel' -- Normalize marital statuts to readable format
					 when Upper (trim(cst_meterial_status)) ='M' then 'Married'
					 else 'n/a'
					 end cst_meterial_status,

				  case when Upper (trim(cst_gendr)) ='F' then 'Female' --Normalize gender to readable format
					 when Upper (trim(cst_gendr)) ='M' then 'Male'
					 else 'n/a'
					 end cst_gendr,
				  cst_create_date
				  from
			 (
			 SELECT *,
			 ROW_NUMBER() over (partition by cst_id Order by cst_create_date desc) as last_Recourd
				FROM bronze.crm_cust_info
				) as t 
					where t.last_Recourd =1; -- select the most resnt recourd per customer
 
				SET @end_Load_time = GETDATE();
				PRINT 'Load Duration : ' + CAST(DATEDIFF(SECOND, @start_Load_time, @end_Load_time) AS NVARCHAR(20)) + ' seconds';
				PRINT '--------------------';

				----------------------------------------------------------
				-- CRM: prd_info
				----------------------------------------------------------
	 
				set @start_Load_time = GETDATE();
			print 'truncate table : silver.crm_prd_info';
			truncate table silver.crm_prd_info;
			print 'insert into table :silver.crm_prd_info';
			insert into silver.crm_prd_info
			(
			prd_id,
			cat_id,
			prd_key,
			prd_nm,
			prd_cost,
			prd_line,
			prd_start_dt,
			prd_end_dt
			)

			SELECT 
			prd_id,
			replace (substring (prd_key,1,5),'-','_') as cat_id, --Extract category ID 
			SUBSTRING(prd_key ,7,len(prd_key)) as prd_key,--Extract Product Key
			prd_nm,
			isnull(prd_cost,0)as prd_cost,
			case  upper(trim (prd_line)) 
				 when 'M' then 'Mountain'
				 when 'R' then 'Road'
				 when 'S' then 'Other Sales'
				 when 'T' then 'Touring'
				 else 'n/a' 
				 end as prd_line,--Map Product line codes to descriptive values
			cast (prd_start_dt as date) as prd_start_dt ,
			cast (lead (prd_start_dt)over (Partition by prd_key order by prd_start_dt)-1 
			as date) as prd_end_dt -- Calculate end date as one day before the next start date
			FROM bronze.crm_prd_info;
		
				SET @end_Load_time = GETDATE();
				PRINT 'Load Duration : ' + CAST(DATEDIFF(SECOND, @start_Load_time, @end_Load_time) AS NVARCHAR) + ' seconds';
				PRINT '--------------------';

				----------------------------------------------------------
				-- CRM: sales_details
				----------------------------------------------------------
	 
				set @start_Load_time = GETDATE();

			print 'truncate table : silver.crm_sales_details';
			truncate table silver.crm_sales_details;
			print 'insert into table :silver.crm_sales_details';
			insert into Silver.crm_sales_details (
				sls_ord_num ,
				sls_prd_key,
				sls_cust_id ,
				sls_order_dt ,
				sls_ship_dt ,
				sls_due_dt ,
				sls_sales ,
				sls_quantit ,
				sls_price 
			)

			select 
			sls_ord_num,
			sls_prd_key,
			sls_cust_id,
			case when sls_order_dt = 0 or len(sls_order_dt) !=8 then null 
				else cast(cast(sls_order_dt as varchar) as date ) 
				end as sls_order_dt,

			case when sls_ship_dt = 0 or len(sls_ship_dt) !=8 then null 
				else cast(cast(sls_ship_dt as varchar) as date ) 
				end as sls_ship_dt,

			case when sls_due_dt = 0 or len(sls_due_dt) !=8 then null 
				else cast(cast(sls_due_dt as varchar) as date ) 
				end as sls_due_dt,
			case when sls_sales  <=0 or sls_sales is null or sls_sales !=  sls_quantit * ABS (sls_price) 
					then sls_quantit *ABS (sls_price) 
					else 
					sls_sales 
						end as sls_sales ,
			sls_quantit,
			case when sls_price  <=0 or sls_price is null 
					then sls_sales/nullif( sls_quantit ,0)
					else 
					sls_price 
						end as sls_price
			from bronze.crm_sales_details;
	    
				SET @end_Load_time = GETDATE();
				PRINT 'Load Duration : ' + CAST(DATEDIFF(SECOND, @start_Load_time, @end_Load_time) AS NVARCHAR) + ' seconds';
				PRINT '--------------------';


				----------------------------------
				--  ERP 
				----------------------------------
				PRINT '-----------------------------------';
				PRINT '        Loading ERP Tables';
				PRINT '-----------------------------------';

				----------------------------------------------------------
				-- ERP: cust_az12
				----------------------------------------------------------
		
				set @start_Load_time = GETDATE();

			print 'truncate table : silver.erp_cust_az12';
			truncate table silver.erp_cust_az12;
			print 'insert into table :silver.erp_cust_az12';
			insert into silver.erp_cust_az12(cid,bdate,gen)

			select 
				case when cid like 'NAS%' then SUBSTRING(cid ,4,len(cid))
					else cid 
					end as cid,

				case when bdate > GETDATE() then null
				else bdate
					end as bdate ,

				case when UPPER(trim(gen)) in('F','FEMALE') then 'Female'
					 when UPPER(trim(gen)) in('M','MALE') then 'Male'
					else 'n/a' 
					end as gen
			from bronze.erp_cust_az12;
	 
				SET @end_Load_time = GETDATE();
				PRINT 'Load Duration : ' + CAST(DATEDIFF(SECOND, @start_Load_time, @end_Load_time) AS NVARCHAR) + ' seconds';
				PRINT '--------------------';


				----------------------------------------------------------
				-- ERP: px_cat_g1v2
				----------------------------------------------------------
		
				set @start_Load_time = GETDATE();

			print 'truncate table : silver.erp_loc_a101';
			truncate table silver.erp_loc_a101;
			print 'insert into table : silver.erp_loc_a101';
			insert into silver.erp_loc_a101(cid,cntry)
			select
			replace (cid,'-','')  cid,
			case when trim(cntry)  in ('US' ,'USA') then 'United States'  
				 when trim(cntry)  is NULL or cntry='' then 'n/a'
				 when trim(cntry)= 'DE' then 'Germany'
				 else trim(cntry)  
				 end as cntry
			from [bronze].[erp_loc_a101] ;
		
				SET @end_Load_time = GETDATE();
				PRINT 'Load Duration : ' + CAST(DATEDIFF(SECOND, @start_Load_time, @end_Load_time) AS NVARCHAR) + ' seconds';
				PRINT '--------------------';

				----------------------------------------------------------
				-- ERP: px_cat_g1v2
				----------------------------------------------------------
		
				set @start_Load_time = GETDATE();

			print 'truncate table : silver.erp_px_cat_g1v2';
			truncate table silver.erp_px_cat_g1v2;
			print 'insert into table : silver.erp_px_cat_g1v2';
			insert into silver.erp_px_cat_g1v2(id,cat,subcat,maintenance)
			select 
			id,
			cat,
			subcat,
			maintenance
			from bronze.erp_px_cat_g1v2  order by id;
		
				SET @end_Load_time = GETDATE();
				PRINT 'Load Duration : ' + CAST(DATEDIFF(SECOND, @start_Load_time, @end_Load_time) AS NVARCHAR) + ' seconds';
				PRINT '--------------------';

				
					PRINT 'Load Duration : ' + CAST(DATEDIFF(SECOND, @start_Load_time, @end_Load_time) AS NVARCHAR) + ' seconds';
					PRINT '--------------------';
							set @end_All_load_time = GETDATE();
    
    		PRINT '=============================================';
    		PRINT 'Loading Silver layer is complited ...';
			PRINT 'Load Duration for Silver layer : ' + CAST(DATEDIFF(SECOND, @start_All_load_time, @end_All_load_time) AS NVARCHAR) + ' seconds';
			PRINT '=============================================';
	end try 

	 BEGIN CATCH
        PRINT '======================================';
        PRINT 'Error Occurred when loading Silver layer';
        PRINT 'Error Message : ' + ERROR_MESSAGE();
        PRINT 'Error Number  : ' + CAST(ERROR_NUMBER() AS NVARCHAR);
        PRINT '======================================';
    END CATCH;
end ;



