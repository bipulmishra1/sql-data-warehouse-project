/*
===============================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
===============================================================================
Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files. 
    It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Uses the `BULK INSERT` command to load data from csv Files to bronze tables.

Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC bronze.load_bronze;
===============================================================================
*/


create or alter procedure bronze.load_bronze AS
Begin
	Declare @StartTime DATETIME, @EndTime DATETIME, @batch_starttime DateTime, @batch_end_time DATETIME;
	Begin Try
		set @batch_starttime = GETDATE();
		print '================================================='
		print 'Loading data into Bronze layer tables...'
		print '================================================='
		-- ### Inserting data into tables from CSV files ###

		print '-------------------------------------------------'
		print 'Loading CRM Tables'
		print '-------------------------------------------------'
		--- Truncating tables before inserting data because we may run this script multiple times

		print '>>Truncating Table: bronze.crm_cust_info '

		Set @StartTime = GETDATE();
		
		Truncate Table bronze.crm_cust_info;

		-- Inserting crm_cust_info data into tables from cust_info.CSV files

		print '>>Inserting Data Into: bronze.crm_cust_info '

		Bulk insert bronze.crm_cust_info
		from 'C:\Users\bimal\Bipul\DATA WAREHOUSE PROJECT\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		with
		(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			ROWTERMINATOR = '\n',
			TABLOCK
		);
		set @EndTime = GETDATE();
		print 'Data load time for bronze.crm_cust_info: ' + cast(DATEDIFF(SECOND, @StartTime, @EndTime) As Nvarchar(10))+'seconds';
		print '---------------------------'

		--- Truncating tables before inserting data because we may run this script multiple times
		set @StartTime = GETDATE();

		print '>>Truncating Table: bronze.crm_prd_info '

		Truncate Table bronze.crm_prd_info;

		-- Inserting crm_prd_info data into tables from prd_info.CSV files 

		print '>>Inserting Data Into: bronze.crm_prd_info '

		Bulk insert bronze.crm_prd_info
		from 'C:\Users\bimal\Bipul\DATA WAREHOUSE PROJECT\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		with
		(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			ROWTERMINATOR = '\n',
			TABLOCK
		);
		set @EndTime = GETDATE();
		print 'Data load time for bronze.crm_prd_info: ' + cast(DATEDIFF(SECOND, @StartTime, @EndTime) As Nvarchar(10))+'seconds';
		print '---------------------------'

		--- Truncating tables before inserting data because we may run this script multiple times

		print '>>Truncating Table: bronze.crm_sales_details '

		set @StartTime = GETDATE();

		Truncate Table bronze.crm_sales_details;

		-- Inserting crm_sales_details data into tables from sales_details.CSV files

		print '>>Inserting Data Into: bronze.crm_sales_details '

		Bulk insert bronze.crm_sales_details
		from 'C:\Users\bimal\Bipul\DATA WAREHOUSE PROJECT\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		with
		(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			ROWTERMINATOR = '\n',
			TABLOCK
		);
		set @EndTime = GETDATE();
		print 'Data load time for bronze.crm_sales_details: ' + cast(DATEDIFF(SECOND, @StartTime, @EndTime) As Nvarchar(10))+'seconds';
		

		print '-------------------------------------------------'
		print'Finished loading CRM Tables'
		print '-------------------------------------------------'
	
		print '-------------------------------------------------'
		print 'Loading ERP Tables'
		print '-------------------------------------------------'
		--- Truncating tables before inserting data because we may run this script multiple times

		print '>>Truncating Table: bronze.erp_cust_az12 '
		set @StartTime = GETDATE();
		Truncate Table bronze.erp_cust_az12;

		-- Inserting erp_cust_az12 data into tables from CUST_AZ12.CSV files
		print '>>Inserting Data Into: bronze.erp_cust_az12 '

		Bulk insert bronze.erp_cust_az12
		from 'C:\Users\bimal\Bipul\DATA WAREHOUSE PROJECT\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
		with
		(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			ROWTERMINATOR = '\n',
			TABLOCK
		);

		SET @EndTime = GETDATE();
		print 'Data load time for bronze.erp_cust_az12: ' + cast(DATEDIFF(SECOND, @StartTime, @EndTime) As Nvarchar(10))+'seconds';
		print '---------------------------'

		--- Truncating tables before inserting data because we may run this script multiple times

		print '>>Truncating Table: bronze.erp_loc_a101 '
		
		set @StartTime = GETDATE();

		Truncate Table bronze.erp_loc_a101;

		-- Inserting erp_loc_a101 data into tables from LOC_A101.CSV files
		print '>>Inserting Data Into: bronze.erp_loc_a101 '

		Bulk insert bronze.erp_loc_a101
		from 'C:\Users\bimal\Bipul\DATA WAREHOUSE PROJECT\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
		with
		(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			ROWTERMINATOR = '\n',
			TABLOCK
		);

		set @EndTime = GETDATE();
		print 'Data load time for bronze.erp_loc_a101: ' + cast(DATEDIFF(SECOND, @StartTime, @EndTime) As Nvarchar(10))+'seconds';
		print '---------------------------'

		--- Truncating tables before inserting data because we may run this script multiple times

		print '>>Truncating Table: bronze.erp_px_cat_g1v2 '

		set @StartTime = GETDATE();

		Truncate Table bronze.erp_px_cat_g1v2;

		-- Inserting erp_px_cat_g1v2 data into tables from PX_CAT_G1V2.CSV files

		print '>>Inserting Data Into: bronze.erp_px_cat_g1v2 '

		Bulk insert bronze.erp_px_cat_g1v2
		from 'C:\Users\bimal\Bipul\DATA WAREHOUSE PROJECT\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
		with
		(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',', 
			ROWTERMINATOR = '\n',
			TABLOCK
		);

		set @EndTime = GETDATE();
		print 'Data load time for bronze.erp_px_cat_g1v2: ' + cast(DATEDIFF(SECOND, @StartTime, @EndTime) As Nvarchar(10))+'seconds';
		print '---------------------------'

		print '-------------------------------------------------'
		print'Finished loading ERP Tables'
		print '-------------------------------------------------'

		set @batch_end_time = GETDATE();
		print '================================================='
		print 'Total time taken to load data into Bronze layer tables: ' + cast(DATEDIFF(SECOND, @batch_starttime, @batch_end_time) As Nvarchar(10))+' seconds';
		print '================================================='
	End Try
	Begin Catch
		print '================================================='
		print 'Error occurred while loading data into Bronze layer tables: ' + ERROR_MESSAGE();
		print 'Error Number: ' + CAST(ERROR_NUMBER() AS NVARCHAR(10));
		print 'Error Severity: ' + CAST(ERROR_SEVERITY() AS NVARCHAR(10));
		print 'Error State: ' + CAST(ERROR_STATE() AS NVARCHAR(10));
		print 'Error Line: ' + CAST(ERROR_LINE() AS NVARCHAR(10));
		print '================================================='
	End Catch
End;


