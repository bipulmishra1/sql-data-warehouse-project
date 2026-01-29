/*
===============================================================================
DDL Script: This will create table in database: DataWarehouse

===============================================================================
*/
 
-- Dropping table if already exists

IF OBJECT_ID('silver.crm_cust_info', 'U') IS NOT NULL
	DROP TABLE silver.crm_cust_info;

-- Creating table of COUSTMER INFO

Create table silver.crm_cust_info
(
	cst_id INT,
	cst_key NVARCHAR(50),
	cst_firstname NVARCHAR(100),
	cst_lastname NVARCHAR(100),
	cst_marital_status NVARCHAR(50),
	cst_gndr NVARCHAR(10),
	cst_create_date DATE,
	dwh_create_date DateTime2 default GETDATE()
);

-- Dropping table if already exists

if OBJECT_ID('silver.crm_prd_info', 'U') IS NOT NULL
	DROP TABLE silver.crm_prd_info;

-- Creating table of PRODUCT INFO

Create table silver.crm_prd_info
(
	prd_id INT,
	cat_id NVARCHAR(50),
	prd_key NVARCHAR(50),
	prd_nm NVARCHAR(100),
	prd_cost INT,
	prd_line NVARCHAR(100),
	prd_start_dt DATE,
	prd_end_dt DATE,
	dwh_create_date DateTime2 default GETDATE()
);

-- Dropping table if already exists

if OBJECT_ID('silver.crm_sales_details', 'U') IS NOT NULL
	DROP TABLE silver.crm_sales_details;

-- Creating table of SALES DETAILS

Create table silver.crm_sales_details
(
	sls_ord_num NVARCHAR(50),
	sls_prd_key NVARCHAR(50),
	sls_cust_id INT,
	sls_order_dt Date,
	sls_ship_dt Date,
	sls_due_dt Date,
	sls_sales INT,
	sls_quantity INT,
	sls_price INT,
	dwh_create_date DateTime2 default GETDATE()
);

-- Dropping table if already exists

if OBJECT_ID('silver.erp_cust_az12', 'U') IS NOT NULL
	DROP TABLE silver.erp_cust_az12;

-- Creating table of CUSTOMER 

Create Table silver.erp_cust_az12
(
	cid NVARCHAR(50),
	bdate DATE,
	gen NVARCHAR(50),
	dwh_create_date DateTime2 default GETDATE()
);

-- Dropping table if already exists

if OBJECT_ID('silver.erp_loc_a101', 'U') IS NOT NULL
	DROP TABLE silver.erp_loc_a101;

-- Creating table of location CUSTOMER

Create Table silver.erp_loc_a101
(
	cid NVARCHAR(50),
	cntry NVARCHAR(50),
	dwh_create_date DateTime2 default GETDATE()
);

-- Dropping table if already exists

if OBJECT_ID('silver.erp_px_cat_g1v2', 'U') IS NOT NULL
	DROP TABLE silver.erp_px_cat_g1v2;

-- Creating table of PRODUCT CATEGORY

Create Table silver.erp_px_cat_g1v2
(
	id NVARCHAR(50),
	cat NVARCHAR(50),
	subcat NVARCHAR(50),
	maintenance NVARCHAR(50),
	dwh_create_date DateTime2 default GETDATE()
);

