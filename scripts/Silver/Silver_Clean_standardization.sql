/*
Clean And Load to Silver Layer
This script performs data quality checks, cleaning, and 
    transformation of customer data from the Bronze layer 
    and loads the cleaned dataset into the Silver layer.

Process Overview:
    1. Validate data quality in the Bronze layer:
        - Detect duplicate or null primary keys (cst_id)
        - Identify unwanted spaces in text fields
    2. Standardize and clean data:
        - Trim leading/trailing spaces
        - Normalize marital status and gender values
    3. Deduplicate records:
        - Keep the most recent record per customer using ROW_NUMBER()
    4. Load cleaned data into the Silver table:
        - Truncate the target table
        - Insert transformed and deduplicated records

Source Table:
    bronze.crm_cust_info

Target Table:
    silver.crm_cust_info
*/


-- ============================================
-- Clean, Transform and Load into Silver Layer 
-- ============================================
use data_warehouse
go
create or alter procedure silver.load_procedure as
begin

	declare @start_time_crm datetime, @end_time_crm datetime , @start_time_erp datetime , @end_time_erp datetime , @start_time_prc datetime , @end_time_prc datetime

-- ============================================
-- silver.crm_cust_info
-- ============================================
	set @start_time_prc = GETDATE();
	set @start_time_crm = GETDATE();
	print '>> Truncating the table: silver.crm_cust_info';

		truncate table silver.crm_cust_info;  	-- elimina registros antes de insertar
	print '>> Inserting Data into: silver.crm_cust_info';
		with  cst_id_base as (select*
		,row_number() over (partition by cst_id order by  cst_create_date desc) as flag_last
		from bronze.crm_cust_info
		)
		
		insert into silver.crm_cust_info (
			cst_id,
			cst_key,
			cst_firstname,
			cst_lastname,
			cst_marital_status,
			cst_gndr,
			cst_create_date
		)

		select	cst_id, 
				cst_key, 
				trim(cst_firstname) as cst_fisrt_name, 
				trim(cst_lastname) as cst_last_name, 
				case when upper(trim(cst_marital_status)) = 'S' then 'Single'
					when upper(trim(cst_marital_status)) = 'M' then 'Married'
					else 'Unknown'
				end cst_marital_status,	
				case when upper(trim(cst_gndr)) ='F' then 'Female'
					when upper(trim(cst_gndr)) ='M' then 'Male'
					else 'N/A'
					end cst_gndr,		
				cst_create_date

		from cst_id_base
		where flag_last = 1;

	/*
	select cst_id, count(*)
	from silver.crm_cust_info
	group by cst_id
	having count(*) > 1


	select cst_lastname
	from silver.crm_cust_info 
	where cst_lastname != trim(cst_lastname)

	select *
	from silver.crm_cust_info */

	-- ============================================
	-- silver.crm_prd_info
	-- ============================================
	print '>> Truncating the table: silver.crm_prd_info';
	truncate table silver.crm_prd_info;
	print '>> Inserting Data into: silver.crm_prd_info';
	insert into silver.crm_prd_info (
			prd_id,
			cat_id ,
			prd_key ,
			prd_nm ,
			prd_cost,
			prd_line,
			prd_start_dt,
			prd_end_dt
		
			)
			
		select 
			prd_id,
			replace(substring(prd_key,1,5),'-','_') as cat_id,
			substring(prd_key, 7 , len(prd_key)) as prd_key,
			prd_nm,
			isnull(prd_cost,0) as prd_cost, -- function is null for replace null values
			case upper(trim(prd_line))
				when 'R' then 'Road'
				when 'S' then 'Other Sales'
				when 'M' then 'Mountain'
				when 'T' then 'Touring'
				else  'N/A'
				end as prd_line,		
			cast (prd_start_dt as date) as prd_start_dt, -- cast para borrar formato de hora en la variable
			cast(lead(prd_start_dt) over(partition by prd_key order by prd_start_dt) - 1 as date) as prd_end_dt  -- Lead para tomar el siguiente registro
			from bronze.crm_prd_info
		/*where replace(substring(prd_key,1,5),'-','_')  not in  -- cruza para validar que cat_id no están en tabla bronze.erp_px_cat_g1v2
		(select distinct id from bronze.erp_px_cat_g1v2)*/

		/* where substring(prd_key, 7 , len(prd_key)) not in (select sls_prd_key from bronze.crm_sales_details) cruza para validar que cat_id no están en tabla bronze.crm_sales_details se puede suar in o not in dependiendo de lo que se busque
		*/


	-- ============================================
	-- silver.crm_sales_details
	-- ============================================
	print '>> Truncating the table: silver.crm_sales_details'
	
	truncate table silver.crm_sales_details;
	print '>> Inserting Data into: silver.crm_sales_details'
	
	insert into silver.crm_sales_details(
				sls_ord_num,
				sls_prd_key,
				sls_cust_id,
				sls_order_dt,
				sls_ship_dt,
				sls_due_dt,
				sls_sales,
				sls_quantity,
				sls_price


				)

	select	sls_ord_num,
			sls_prd_key,
			sls_cust_id,
			case when sls_order_dt<= 0 or len(sls_order_dt) != 8 then null
					else cast(cast(sls_order_dt as varchar) as date)
					end as sls_order_dt,
			case when sls_ship_dt<= 0 or len(sls_ship_dt) != 8 then null
					else cast(cast(sls_ship_dt as varchar)as date)
					end as sls_ship_dt,
			case when sls_due_dt<= 0  or len(sls_due_dt) != 8 then null
					else cast(cast(sls_due_dt as varchar) as date)
					end as sls_due_dt,	
			case  when sls_sales is null  or sls_sales <0 or sls_sales != sls_quantity * abs(sls_price) then sls_quantity * abs(sls_price)
			else sls_sales end as sls_sales,
			sls_quantity,
			case when sls_price is null or sls_price <0 then sls_sales / nullif(sls_quantity,0) else sls_price
			end as sls_price
		
				
	from bronze.crm_sales_details

	/*select * 
	from silver.crm_sales_details
	where 1 = 0*/
	set @end_time_crm = getdate()
	print '==================================================='
	print 'CRM Load data time in silver layer: ' + cast(datediff(millisecond,@start_time_crm , @end_time_crm) as varchar) + ' Milliseconds'
	print '==================================================='
	-- ============================================
	-- silver.erp_cust_az12
	-- ============================================

	set @start_time_erp = GETDATE();
	print '>> Truncating the table: silver.erp_cust_az12'

	truncate table silver.erp_cust_az12;
	print '>> Inserting Data into: silver.erp_cust_az12'
	insert into silver.erp_cust_az12(
			cid,
			bdate,
			gen
			)
	select 
		case  when cid like 'NAS%' then SUBSTRING(cid,4,len(cid))
		else cid
		end as cid,
		case when bdate > getdate() then null
		else bdate
		end as bdate,
		case when upper(trim(gen))  in ('F', 'FEMALE') then 'Female'
		when upper(trim(gen)) In  ('M', 'MALE') then 'Male'
		else 'N/A' 
		end as gen
		from bronze.erp_cust_az12

	--select * from silver.erp_cust_az12

	-- ============================================
	-- silver.erp_loc_a101
	-- ============================================

	print '>> Truncating the table: silver.erp_loc_a101'
	truncate table silver.erp_loc_a101;
	print '>> Inserting Data into: silver.erp_loc_a101'
	insert into silver.erp_loc_a101 (
			cid,
			cntry
			)	
	select
		replace(cid,'-','') as cid,	
		case when upper(trim(cntry)) in ('USA','US','UNITED STATES') then 'USA'
		when upper(trim(cntry)) = '' or upper(trim(cntry)) is null  then 'N/A'
			when upper(trim(cntry)) = 'DE' then 'GERMANY'
		else upper(trim(cntry))
		end as cntry
	from bronze.erp_loc_a101
	order by cntry


	-- ============================================
	-- silver.erp_px_cat_g1v2
	-- ============================================

	print '>> Truncating the table: silver.erp_px_cat_g1v2'
	truncate  table silver.erp_px_cat_g1v2;
	print '>> Inserting Data into: silver.erp_px_cat_g1v2'
	insert into silver.erp_px_cat_g1v2 (
					id,
					cat,
					subcat,
					maintenance)
		select * from bronze.erp_px_cat_g1v2
	set @end_time_erp = GETDATE();
	set @end_time_prc = GETDATE();
	print '==================================================='
	print 'ERP Load data time in Silver Layer: ' + cast(datediff(millisecond, @start_time_erp,@end_time_erp) as varchar)  + ' Milliseconds'
	print '==================================================='
	print '                                     '
	print '==================================================='
	print 'Time Load duration Silver Layer: ' + cast(datediff(millisecond, @start_time_prc,@end_time_prc) as varchar)  + ' Milliseconds'
	print '==================================================='
end
