use data_warehouse
select * 
from bronze.erp_loc_a101


--========================
-- check and validation
--========================

-- replace in keys
select replace(cid,'-','') as cid
from bronze.erp_loc_a101

-- check for  keys model

select replace(cid,'-','') as cid
from bronze.erp_loc_a101
where replace(cid,'-','') not in (select distinct cst_key from silver.crm_cust_info)


select distinct cst_key from silver.crm_cust_info
-- check duplicates

select cid, count(*) as conteo
from bronze.erp_loc_a101
group by cid
having count(*) > 1

-- check for unwanted spaces

select cid 
from bronze.erp_loc_a101
where cid != trim(cid)

-- Check for distinct values in category variables

select distinct
	cntry,
	case when upper(trim(cntry)) in ('USA','US','UNITED STATES') then 'USA'
	when upper(trim(cntry)) = '' or upper(trim(cntry)) is null  then 'N/A'
		when upper(trim(cntry)) = 'DE' then 'GERMANY'
	else upper(trim(cntry))
	end as cntry_tf
from bronze.erp_loc_a101 -- identificación de valores que no coinciden

-- ============================================
-- Query Cleaning, Transformation and Insert
-- ============================================
truncate table silver.erp_loc_a101;
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

--select * from silver.erp_loc_a101