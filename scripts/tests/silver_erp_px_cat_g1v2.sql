use data_warehouse
select *
from bronze.erp_px_cat_g1v2

--========================
-- check and validation
--========================
-- check for keys

select id 
from bronze.erp_px_cat_g1v2
where id not  in (select cat_id from silver.crm_prd_info)
order by id desc

select cat_id from silver.crm_prd_info
order  by cat_id desc

--Check for duplicates
select id, count(*) as conteo
from bronze.erp_px_cat_g1v2
group by id
having count(*) >= 2

-- check for unwanted spaces

select * 
from bronze.erp_px_cat_g1v2
where trim(cat) != cat or trim(subcat) != subcat or trim(maintenance) != maintenance -- no hay unwanted spaces

-- check for distinct values in categoric variables

select distinct subcat
from bronze.erp_px_cat_g1v2

-- ============================================
-- Query Cleaning, Transformation and Insert
-- ============================================
print '>> Truncating the table: silver.erp_px_cat_g1v2 '
truncate  table silver.erp_px_cat_g1v2;
print '>> Inserting Data into: silver.erp_px_cat_g1v2 '
insert into silver.erp_px_cat_g1v2 (
				id,
				cat,
				subcat,
				maintenance)
	select * from bronze.erp_px_cat_g1v2