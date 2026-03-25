
use data_warehouse
--========================
-- checks and validation
--========================
select *
from bronze.crm_sales_details
where sls_ord_num != trim(sls_ord_num)

-- check prd key coincidence
select *
from bronze.crm_sales_details
where sls_prd_key not in (select prd_key from silver.crm_prd_info) -- Validamos que todos los keys de sls_prd_key se encuentran en crm_prd_info

-- check prd key coincidence
select *
from bronze.crm_sales_details
where sls_cust_id not in (select cst_id from silver.crm_cust_info) -- Validamos que todos los keys de sls_prd_key se encuentran en crm_prd_info
 
 -- check date data

 select sls_order_dt
 from silver.crm_sales_details
 where sls_order_dt  <=0 -- fechas inexistentes reemplazar por null
	or sls_order_dt > 20260101 -- fechas posteriores a la actualidad
	or sls_order_dt < 19000101 -- fechas inferiores a la primera fecha sql
	or len(sls_order_dt) != 8  -- fechas con menos de 8 digitos

	select *
	from bronze.crm_sales_details 
	where sls_order_dt >sls_ship_dt or sls_order_dt > sls_due_dt   -- Verificar que el orden cronologico de las ordenes 

	 -- check date data en silver

 select sls_order_dt
 from silver.crm_sales_details
 where sls_order_dt  is null -- fechas inexistentes reemplazar por null
	or sls_order_dt > '2026-01-01' -- fechas posteriores a la actualidad
	or sls_order_dt < '1900-01-01' -- fechas inferiores a la primera fecha sql
	
select *
	from silver.crm_sales_details 
	where sls_order_dt >sls_ship_dt or sls_order_dt > sls_due_dt   -- Verificar que el orden cronologico de las ordenes




	-- Check data consistency: sales , quantity and price

	select distinct  sls_sales as old_sls_sales , sls_quantity, sls_price as old_sls_price,
	case  when sls_sales is null  or sls_sales <0 or sls_sales != sls_quantity * abs(sls_price) then sls_quantity * abs(sls_price)
		else sls_sales end as sls_sales,
		case when sls_price is null or sls_price <0 then sls_sales / nullif(sls_quantity,0) else sls_price
		end as sls_price
	from bronze.crm_sales_details
	where sls_sales != sls_quantity * sls_price
	or sls_sales is null or sls_quantity is null or sls_price  is null
	or sls_sales <0 or sls_quantity <0 or sls_price <0
	order by sls_sales,sls_quantity,sls_price 
-- ============================================
-- Query Cleaning, Transformation and Insert
-- ============================================

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



select * 
from bronze.crm_sales_details
where 1 = 0 


 