/*
Description:
This stored procedure creates the required tables for the Data Warehouse in the silver layer.
It first checks if each table already exists and drops it if necessary, then recreates the tables
to ensure the correct structure.

The procedure creates tables for both CRM and ERP source systems within the silver schema.
These tables are intended to store raw ingested data before further transformations in higher
layers of the DWH (e.g., Silver or Gold).
*/

use data_warehouse
go

create or alter procedure silver.create_dwh_tables as
begin
-- Creación de tablas DWH

	print '==================';
	print 'Create DWH Tables';
	print '==================';


	print '------------------';
	print 'silver.crm tables';
	print '------------------';

	if OBJECT_ID('silver.crm_cust_info','U') is not null
		drop table silver.crm_cust_info;
	create  table silver.crm_cust_info (
		cst_id int,
		cst_key nvarchar(50),
		cst_firstname nvarchar(50),
		cst_lastname nvarchar(50),
		cst_marital_status nvarchar(50),
		cst_gndr nvarchar(30),
		cst_create_date date,
		dwh_create_date datetime2 default GETDATE()
		);


	if OBJECT_ID('silver.crm_prd_info','U') is not null
		drop table silver.crm_prd_info
	create table silver.crm_prd_info(
		prd_id nvarchar(50),
		prd_key nvarchar(50),
		prd_nm nvarchar(50),
		prd_cost float,
		prd_line nvarchar(20),
		prd_start_dt datetime,
		prd_end_dt datetime,
		dwh_create_date datetime2 default GETDATE()

		);

	if  OBJECT_ID('silver.crm_sales_details','U') is not null
		drop table silver.crm_sales_details; 
	create table silver.crm_sales_details(
		sls_ord_num nvarchar(50),
		sls_prd_key nvarchar(50),
		sls_cust_id int,
		sls_order_dt int,
		sls_ship_dt int,
		sls_due_dt int,
		sls_sales  int,
		sls_quantity int,
		sls_price float,
		dwh_create_date datetime2 default GETDATE()
	
		);

	print '------------------';
	print 'silver.erp tables';
	print '------------------';

	if OBJECT_ID('silver.erp_cust_az12','U') is not null
		drop table silver.erp_cust_az12;
	create table silver.erp_cust_az12(
		cid nvarchar(50),
		bdate date,
		gen nvarchar(20),
		dwh_create_date datetime2 default GETDATE()
		);


	if OBJECT_ID('silver.erp_loc_a101','U') is not null
		drop table silver.erp_loc_a101;
	create table silver.erp_loc_a101(
		cid nvarchar(50),
		cntry nvarchar(50),
		dwh_create_date datetime2 default GETDATE()
		);



	if OBJECT_ID('silver.erp_px_cat_g1v2','U') is not null
		drop table silver.erp_px_cat_g1v2;
	create table silver.erp_px_cat_g1v2(
		id nvarchar(50),
		cat nvarchar(50),
		subcat nvarchar(50),
		maintenance nvarchar(10),
		dwh_create_date datetime2 default GETDATE()
	
		);

end
