/*
==================================================================================================
DDL SCRIPT : CREATE BRONZE TABLES
=================================================================================================
SCRIPT PURPOSE:
THIS SCRIPT CREATES TABLES IN THE BRONZE SCHEMA , DROPPING EXISTING TABLES IF THEY ALREADY EXISTS.
RUN THIS SCRIPT TO REDEFINE THE DDL STRUCTURE OF 'BRONZE' TABLES.
==================================================================================================
*/

IF OBJECT_ID('Bronze.crm_cust_info','U') IS NOT NULL
   DROP TABLE Bronze.crm_cust_info;

CREATE TABLE Bronze.crm_cust_info(
cst_id INT,
cst_key NVARCHAR(50),
cst_firstname NVARCHAR(50),
cst_lastname NVARCHAR(50),
cst_marital_status NVARCHAR(50),
cst_gndr NVARCHAR(50),
cst_create_date DATE
);

IF OBJECT_ID('Bronze.crm_prd_info','U') IS NOT NULL
   DROP TABLE Bronze.crm_prd_info;

CREATE TABLE Bronze.crm_prd_info(
prd_id INT,
prd_key NVARCHAR(50),
prd_nm NVARCHAR(50),
prd_cost INT,
prd_line NVARCHAR(50),
prd_start_dt DATETIME,
prd_end_dt DATETIME
);

IF OBJECT_ID('Bronze.crm_sales_details','U') IS NOT NULL
   DROP TABLE Bronze.crm_sales_details;

CREATE TABLE Bronze.crm_sales_details(
sls_ord_num NVARCHAR(50),
sls_prd_key NVARCHAR(50),
sls_cust_id INT,
sls_order_dt INT,
sls_ship_dt INT,
sls_due_dt INT,
sls_sales INT,
sls_quantity INT,
sls_price INT
);

IF OBJECT_ID('Bronze.erp_loc_a101','U') IS NOT NULL
   DROP TABLE Bronze.erp_loc_a101;

CREATE TABLE Bronze.erp_loc_a101(
cid NVARCHAR(50),
cntry NVARCHAR(50));

IF OBJECT_ID('Bronze.erp_cust_az12','U') IS NOT NULL
   DROP TABLE Bronze.erp_cust_az12;

CREATE TABLE Bronze.erp_cust_az12(
cid NVARCHAR(50),
bdate VARCHAR(50),
gen NVARCHAR(50));


IF OBJECT_ID('Bronze.erp_px_cat_g1v2','U') IS NOT NULL
   DROP TABLE Bronze.erp_px_cat_g1v2;

CREATE TABLE Bronze.erp_px_cat_g1v2(
id NVARCHAR(50),
cat NVARCHAR(50),
subcat NVARCHAR(50),
maintenance NVARCHAR(50));

CREATE OR ALTER PROCEDURE Bronze.load_Bronze AS
BEGIN 
     DECLARE @start_time DATETIME, @end_time DATETIME,@batch_start_time DATETIME,@batch_end_time DATETIME;
     BEGIN TRY
     SET @batch_start_time = GETDATE();
     PRINT '=============================';
     PRINT 'LOADING THE BRONZE LAYER';
     PRINT '=============================';
     
     PRINT '--------------------------';
     PRINT 'LOADING CRM TABLES';
     PRINT '--------------------------';
     
     SET @start_time = GETDATE();

     PRINT '>> TRUNCATING TABLE:BRONZE.CRM_CUST_INFO';
     TRUNCATE TABLE Bronze.crm_cust_info
     BULK INSERT Bronze.crm_cust_info
     FROM 'C:\Users\Hp\OneDrive\Desktop\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
     WITH (
          FIRSTROW=2,
          FIELDTERMINATOR = ',',
          TABLOCK
     ); 
     SET @end_time = GETDATE();
     PRINT '>> LOAD DURATION:'+ CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR)+ ' seconds';

     SET @start_time = GETDATE();

     PRINT '>> TRUNCATING TABLE:BRONZE.CRM_PRD_INFO';
     TRUNCATE TABLE Bronze.crm_prd_info
     BULK INSERT Bronze.crm_prd_info
     FROM 'C:\Users\Hp\OneDrive\Desktop\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
     WITH (
          FIRSTROW=2,
          FIELDTERMINATOR = ',',
          TABLOCK
     );
      SET @end_time = GETDATE();
      PRINT '>> LOAD DURATION:'+ CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR)+ ' seconds';

     SET @start_time = GETDATE();
     PRINT '>> TRUNCATING TABLE:BRONZE.CRM_SALES_DETAILS';
     TRUNCATE TABLE Bronze.crm_sales_details
     BULK INSERT Bronze.crm_sales_details
     FROM 'C:\Users\Hp\OneDrive\Desktop\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
     WITH (
          FIRSTROW=2,
          FIELDTERMINATOR = ',',
          TABLOCK
     );
      SET @end_time = GETDATE();
      PRINT '>> LOAD DURATION:'+ CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR)+ ' seconds';
     
     PRINT '--------------------------';
     PRINT 'LOADING ERP TABLES';
     PRINT '--------------------------';

     SET @start_time = GETDATE();
     PRINT '>> TRUNCATING TABLE:BRONZE.ERP_CUST_AZ12';
     TRUNCATE TABLE Bronze.erp_cust_az12
     BULK INSERT Bronze.erp_cust_az12
     FROM 'C:\Users\Hp\OneDrive\Desktop\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
     WITH (
         FIRSTROW=2,
         FIELDTERMINATOR = ',',
         TABLOCK
     );
      SET @end_time = GETDATE();
      PRINT '>> LOAD DURATION:'+ CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR)+ ' seconds';

     SET @start_time = GETDATE();
     PRINT '>> TRUNCATING TABLE:BRONZE.ERP_LOC_A101';
     TRUNCATE TABLE Bronze.erp_loc_a101
     BULK INSERT Bronze.erp_loc_a101
     FROM 'C:\Users\Hp\OneDrive\Desktop\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
     WITH (
         FIRSTROW=2,
         FIELDTERMINATOR = ',',
         TABLOCK
     );
      SET @end_time = GETDATE();
      PRINT '>> LOAD DURATION:'+ CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR)+ ' seconds';

     SET @start_time = GETDATE();
     PRINT '>> TRUNCATING TABLE:BRONZE.ERP_PX_CAT_G1V2';
     TRUNCATE TABLE Bronze.erp_px_cat_g1v2
     BULK INSERT Bronze.erp_px_cat_g1v2
     FROM 'C:\Users\Hp\OneDrive\Desktop\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
     WITH (
         FIRSTROW=2,
         FIELDTERMINATOR = ',',
         TABLOCK
     );
      SET @end_time = GETDATE();
      PRINT '>> LOAD DURATION:'+ CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR)+ ' seconds';

      SET @batch_end_time=GETDATE();
      PRINT '==================================='
      PRINT 'LOADING BRONZE LAYER IS COMPLETED.';
      PRINT 'TOTAL LOAD DURATION:'+CAST(DATEDIFF(SECOND,@batch_start_time,@batch_end_time)AS NVARCHAR)+' seconds';
      PRINT '==================================='
     END TRY
     BEGIN CATCH
          PRINT '===========================================';
          PRINT ' ERROR OCCURED DURING LOADING BRONZE LAYER ';
          PRINT 'ERROR MESSSAGE'+ERROR_MESSAGE();
          PRINT ' ERROR MESSAGE '+ CAST(ERROR_NUMBER() AS NVARCHAR);
          PRINT 'ERROR MESSAGE '+ CAST(ERROR_STATE() AS NVARCHAR);
          PRINT '============================================';
     END CATCH
END;

EXEC Bronze.load_Bronze
