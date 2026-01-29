/*
===============================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
===============================================================================
Script Purpose:
    This stored procedure performs the ETL (Extract, Transform, Load) process to 
    populate the 'silver' schema tables from the 'bronze' schema.
	Actions Performed:
		- Truncates Silver tables.
		- Inserts transformed and cleansed data from Bronze into Silver tables.
		
Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC Silver.load_silver;
===============================================================================
*/


-- storing the table in store procedure for fast access

CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
	Declare @StartTime DATETIME, @EndTime DATETIME, @batch_starttime DateTime, @batch_end_time DATETIME;
	Begin Try
		set @batch_starttime = GETDATE();
		print '================================================='
		print 'Loading data into silver layer tables...'
		print '================================================='

	-- Inserting data to silver layer table after cleaning the data from bronze layer

		print '-------------------------------------------------'
		print 'Loading CRM Tables'
		print '-------------------------------------------------'

	-- Removing existing for table if their any to don't do overwrite.
	
	Set @StartTime = GETDATE();

	truncate table silver.crm_cust_info;

	--- then inserting data to silver.crm_cust_info table.

	with DuplicateCust as 
	( select *,
		ROW_NUMBER() over(partition by cst_id order by cst_create_date DESC)as ord_id
	from bronze.crm_cust_info where cst_id is not null
	)
	insert  into silver.crm_cust_info(
	cst_id,
	cst_key,
	cst_firstname,
	cst_lastname,
	cst_marital_status,
	cst_gndr,
	cst_create_date)

	select 
	cst_id,
	cst_key,
	trim(cst_firstname) as cst_firstname,
	trim(cst_lastname) as cst_lastname,
	case
	when trim(upper(cst_marital_status)) = 'M' then 'Married'
	when trim(upper(cst_marital_status))  = 'S' then 'Single'
	when trim(upper(cst_marital_status))  is null then 'n/a'
	end as cst_marital_status,
	case 
	when trim(upper(cst_gndr)) = 'M' then 'Male'
	when trim(upper(cst_gndr)) = 'F' then 'Female'
	when trim(Upper(cst_gndr)) is null then 'n/a'
	end as cst_gndr,
	cst_create_date
	from DuplicateCust;

	set @EndTime = GETDATE();
		print 'Data load time for silver.crm_cust_info: ' + cast(DATEDIFF(SECOND, @StartTime, @EndTime) As Nvarchar(10))+'seconds';
		print '---------------------------'

	-- Removing existing for table if their any to don't do overwrite. 

	set @StartTime = GETDATE();

	truncate table silver.crm_prd_info;

	--- then inserting data to silver.crm_prd_info table.

	insert into silver.crm_prd_info(
	prd_id,
	cat_id,
	prd_key,
	prd_nm,
	prd_cost,
	prd_line,
	prd_start_dt,
	prd_end_dt)


	select prd_id,
	upper(replace(SUBSTRING(prd_key,1,5),'-','_')) as cat_id,
	upper(replace(SUBSTRING(prd_key,7),'-','_'))as prd_key,
	prd_nm,
	isnull (prd_cost, 0) as prd_cost,
	case
	when upper(prd_line) = 'M' then 'Mountain'
	when upper(prd_line) = 'R' then 'Road'
	when upper(prd_line) = 's' then 'Other Sales'
	when upper(prd_line) = 'T' then 'Touring'
	when upper(prd_line) is null then 'n/a'
	end as prd_line,
	prd_start_dt,
	dateadd(day,-1,LEAD(prd_start_dt) 
	over(partition by prd_key order by prd_start_dt)) as prd_end_dt
	from bronze.crm_prd_info;

	set @EndTime = GETDATE();
		print 'Data load time for silver.crm_prd_info: ' + cast(DATEDIFF(SECOND, @StartTime, @EndTime) As Nvarchar(10))+'seconds';
		print '---------------------------'

	-- Removing existing for table if their any to don't do overwrite. 
	set @StartTime = GETDATE();

	truncate table silver.crm_sales_details;

	--- then inserting data to silver.crm_sales_details table.

	insert into silver.crm_sales_details(
	sls_ord_num,
	sls_prd_key,
	sls_cust_id,
	sls_order_dt,
	sls_ship_dt,
	sls_due_dt,
	sls_sales,
	sls_quantity,
	sls_price)

	select 
	sls_ord_num,
	replace (sls_prd_key, '-', '_') as sls_prd_key,
	sls_cust_id,
	case 
		when sls_order_dt = 0 or len(sls_order_dt) != 8 then null
		else cast(cast(sls_order_dt as varchar)as DATE)
		end as sls_order_dt,
	case 
		when sls_ship_dt = 0 or len(sls_ship_dt) != 8 then null
		else cast(cast(sls_ship_dt as varchar)as DATE)
	end as sls_ship_dt,
	case
		when sls_due_dt = 0 or len(sls_due_dt) != 8 then null
		else cast(cast(sls_due_dt as varchar)as DATE)
	end as sls_due_dt,
	case 
		when  sls_sales is null or sls_sales != abs(sls_price)*abs (sls_quantity) 
		then abs(sls_price)*abs (sls_quantity)
		when sls_price is null and sls_sales is null or 
		sls_sales is null  and sls_quantity is null  then  0
	else abs(sls_sales)
	end sls_sales,
	case 
		when sls_quantity is null then abs(sls_sales)/abs(sls_price)
		when sls_quantity is null and sls_sales is null or
		sls_quantity is null and sls_price is null then 0
		else abs(sls_quantity)
	end sls_quantity,
	case 
		when sls_price  is null then abs(sls_sales)/abs(sls_quantity)
		when sls_price is null and sls_sales is null or 
		sls_quantity is null and sls_price is null then 0
		else abs(sls_price)
	end as sls_price
	from bronze.crm_sales_details;

	set @EndTime = GETDATE();
		print 'Data load time for silver.crm_sales_details: ' + cast(DATEDIFF(SECOND, @StartTime, @EndTime) As Nvarchar(10))+'seconds';
		print '---------------------------'

	
		print '-------------------------------------------------'
		print'Finished loading CRM Tables'
		print '-------------------------------------------------'
	
		print '-------------------------------------------------'
		print 'Loading ERP Tables'
		print '-------------------------------------------------'

	-- Removing existing for table if their any to don't do overwrite. 
		
	set @StartTime = GETDATE();

	truncate table silver.erp_cust_az12;

	--- then inserting data to silver.erp_cust_az12 table.

	insert into silver.erp_cust_az12(
	cid,
	bdate,
	gen)

	select 
	case
	when cid like '%NAS%' then SUBSTRING(cid,4)
	else cid
	end as cid,
	case
		when bdate < '1926-01-01'or bdate > GETDATE() then Null
		else bdate
		end as bdate,
	case 
		when upper(trim(gen)) in( 'M','MALE') then 'Male'
		When Upper(trim(gen)) in ('F','FEMALE') then 'Female'
		when gen is null then trim('Unknown')
		when len(gen) = 0 then 'Unknown'
		else gen
	end as gen
	from bronze.erp_cust_az12 ;

	SET @EndTime = GETDATE();
		print 'Data load time for silver.erp_cust_az12: ' + cast(DATEDIFF(SECOND, @StartTime, @EndTime) As Nvarchar(10))+'seconds';
		print '---------------------------'

	-- Removing existing for table if their any to don't do overwrite. 

	set @StartTime = GETDATE();

	truncate table silver.erp_loc_a101;

	--- then inserting data to silver.erp_loc_a101 table.

	insert into silver.erp_loc_a101(
	cid,
	cntry)
	select
	replace(cid,'-','') as cid,
	case
		when cntry in ('USA','US','UNITED STATES','united states') then 'United States'
		when cntry in ('DE','GERMANY','germany') then 'Germany'
		when cntry is null or cntry = ' ' then 'Unknown'
		else cntry
	end as cntry
	from bronze.erp_loc_a101;

	set @EndTime = GETDATE();
	print 'Data load time for silver.erp_loc_a101: ' + cast(DATEDIFF(SECOND, @StartTime, @EndTime) As Nvarchar(10))+'seconds';
	print '---------------------------'


	-- Removing existing for table if their any to don't do overwrite. 

	set @StartTime = GETDATE();

	truncate table silver.erp_px_cat_g1v2;

	--- then inserting data to silver.erp_px_cat_g1v2 table.

	insert into silver.erp_px_cat_g1v2(
	id,
	cat,
	subcat,
	maintenance)
	select id,
	cat,
	subcat,
	maintenance
	from bronze.erp_px_cat_g1v2;
	set @EndTime = GETDATE();
		print 'Data load time for silver.erp_px_cat_g1v2: ' + cast(DATEDIFF(SECOND, @StartTime, @EndTime) As Nvarchar(10))+'seconds';
		print '---------------------------'

		print '-------------------------------------------------'
		print'Finished loading ERP Tables'
		print '-------------------------------------------------'

		set @batch_end_time = GETDATE();
		print '================================================='
		print 'Total time taken to load data into silver layer tables: ' + cast(DATEDIFF(SECOND, @batch_starttime, @batch_end_time) As Nvarchar(10))+' seconds';
		print '================================================='
	End Try
	Begin Catch
		print '================================================='
		print 'Error occurred while loading data into silver layer tables: ' + ERROR_MESSAGE();
		print 'Error Number: ' + CAST(ERROR_NUMBER() AS NVARCHAR(10));
		print 'Error Severity: ' + CAST(ERROR_SEVERITY() AS NVARCHAR(10));
		print 'Error State: ' + CAST(ERROR_STATE() AS NVARCHAR(10));
		print 'Error Line: ' + CAST(ERROR_LINE() AS NVARCHAR(10));
		print '================================================='
	End Catch
END
