/* 
==============================================
STORED PROCEDURE :- LOAD BRONZE LAYER
=============================================
*/
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
