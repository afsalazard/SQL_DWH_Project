/*
Description:
This stored procedure loads raw data into the Bronze layer of the Data Warehouse.

The process truncates the existing tables and performs bulk inserts from CSV files
into the corresponding CRM and ERP tables within the bronze schema. The procedure
also measures and prints the load duration for each data source and for the overall
Bronze layer load process.

Basic error handling is implemented using TRY...CATCH to capture and display
any issues that occur during the data loading process.
*/



-- Ingesta de datos con bulk insert

create or alter procedure bronze.load_procedure as
begin
	begin try
	
	declare @start_time datetime, @end_time datetime, @batch_start datetime, @batch_end datetime
		print '=====================';
		print 'Load Bronze Layer';
		print '=====================';
	 
		print '-----------------------------------';
		print '>>Loading data in bronze.crm tables';
		print '-----------------------------------';

		-- Ingesta de datos bulk en  tabla bronze.crm_cust_info
		set @batch_start = GETDATE();
		set @start_time = GETDATE();
		
		truncate table bronze.crm_cust_info; -- elimina registros de tabla

		bulk insert bronze.crm_cust_info
		from 'C:\Users\user\Documents\Andres\3. Portafolio\SQL\sql_data_warehouse_project\datasets\source_crm\cust_info.csv'
		with(
			firstrow = 2,
			fieldterminator = ',',
			tablock
				);

				
	    -- Ingesta de datos bulk en tabla bronze.crm_prd_info

		truncate table bronze.crm_prd_info;
		bulk insert bronze.crm_prd_info
		from 'C:\Users\user\Documents\Andres\3. Portafolio\SQL\sql_data_warehouse_project\datasets\source_crm\prd_info.csv'
		with(
			firstrow = 2,
			fieldterminator = ',',
			tablock
			);
		
		
		-- Ingesta de datos bulk en  tabla bronze.crm_sales_details

		truncate table bronze.crm_sales_details;
		bulk insert bronze.crm_sales_details
		from 'C:\Users\user\Documents\Andres\3. Portafolio\SQL\sql_data_warehouse_project\datasets\source_crm\sales_details.csv'
		with(
			firstrow=2,
			fieldterminator = ',',
			tablock
			);
		

		set @end_time = GETDATE();

		print 'Load Duration CRM Sources: ' + cast(datediff(millisecond,@start_time,@end_time) as nvarchar) + ' milliseconds'

		print '-----------------------------------';
		print '>>Loading data in bronze.erp tables ';
		print '-----------------------------------';
		-- Ingesta de datos bulk en  tabla bronze.erp_CUST_AZ12


		set @start_time = GETDATE();

		truncate table bronze.erp_cust_az12;
		bulk insert bronze.erp_cust_az12
		from 'C:\Users\user\Documents\Andres\3. Portafolio\SQL\sql_data_warehouse_project\datasets\source_erp\CUST_AZ12.csv'
		with(
			firstrow =2,
			fieldterminator= ',',
			tablock
	
			);

	
		-- Ingesta de datos bulk en  tabla bronze.erp_LOC_A101

		truncate table bronze.erp_loc_a101;
		bulk insert bronze.erp_loc_a101
		from 'C:\Users\user\Documents\Andres\3. Portafolio\SQL\sql_data_warehouse_project\datasets\source_erp\LOC_A101.csv'
		with(
			firstrow =2,
			fieldterminator= ',',
			tablock
	
			);

	

		-- Ingesta de datos bulk en  tabla bronze.erp_PX_CAT_G1V2

		truncate table bronze.erp_px_cat_g1v2;
		bulk insert bronze.erp_px_cat_g1v2
		from 'C:\Users\user\Documents\Andres\3. Portafolio\SQL\sql_data_warehouse_project\datasets\source_erp\PX_CAT_G1V2.csv'
		with(
			firstrow =2,
			fieldterminator= ',',
			tablock
		
			);
			set @end_time = GETDATE();
			print 'Load Duration ERP Sources: ' + cast(datediff(millisecond,@start_time,@end_time) as nvarchar) + ' milliseconds';

			set @batch_end = GETDATE();

			print '---------------------------'
			print 'Load Duration Bronze Layer: ' + cast(datediff(millisecond, @batch_start,@batch_end) as nvarchar) + ' milliseconds';
	end try
	begin catch
		print 'Error occured during loading bronze layer'
		print 'Error Number: ' + cast(error_number() as varchar);
		print 'Error Message: ' + cast(error_message() as varchar);
		print 'Error Line: ' + cast(error_line() as varchar);
	end catch

end

