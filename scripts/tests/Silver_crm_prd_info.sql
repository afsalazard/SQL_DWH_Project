/* Source Table:
    bronze.crm_prd_info

Target Table:
    silver.crm_prd_info
*/

-- ============================================
-- Select Inicial
-- ============================================
use data_warehouse
select *
from bronze.crm_prd_info

-- ============================================
-- Check for duplicates or null 
-- ============================================
 
 -- Primary Key

 select prd_id, count(*)
 from bronze.crm_prd_info
 group by prd_id
 having count(*) > 1 or prd_id is null

 -- prd_key

 select prd_key, count(*)
from bronze.crm_prd_info
group by prd_key
having count(*) >1 or prd_key is null

-- ============================================
-- Check unwanted spaces
-- ============================================
select prd_nm
from bronze.crm_prd_info
where trim(prd_nm) != prd_nm

select count(*)
from (
select prd_nm
from bronze.crm_prd_info
where trim(prd_nm) != prd_nm) t

-- ============================================
-- Check for nulls and negative values
-- ============================================
select prd_cost
from bronze.crm_prd_info
where prd_cost < 0 or prd_cost is null



-- ============================================
-- prd_end_dt transformation
-- ============================================

select	prd_key,
		prd_nm,
		prd_start_dt,
		prd_end_dt,
		lead(prd_start_dt) over(partition by prd_key order by prd_start_dt) - 1 as prd_end_dt_test

from bronze.crm_prd_info
where prd_key in ('AC-HE-HL-U509-R','AC-HE-HL-U509')



-- ============================================
-- Query Cleansing, Transformation and Insert
-- ============================================
truncate table silver.crm_prd_info;
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
