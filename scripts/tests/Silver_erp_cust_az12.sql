use data_warehouse
select *
from bronze.erp_cust_az12

--========================
-- checks and validation
--========================

-- check for  keys model


select	cid,
		bdate,
		gen
from bronze.erp_cust_az12
where cid like '%AW00011000%' -- Se identifica que en bronze.erp_cust_az12 existe info adicional 'NAS' que no permite conectar tablas


-- Se Verifica que cid está en cst_key de la tabla silver.crm_cust_info
select 
	cid,
	case  when cid like 'NAS%' then SUBSTRING(cid,4,len(cid))
	else cid
	end as cid
	from bronze.erp_cust_az12
	where case  when cid like 'NAS%' then SUBSTRING(cid,4,len(cid))
	else cid
	end  not in (Select distinct cst_key from silver.crm_cust_info)



select *
from bronze.erp_cust_az12 a
join silver.crm_cust_info b
    on try_cast(a.cid as int) = b.cst_id

select 
    column_name,
    data_type,
    character_maximum_length,
    is_nullable
from information_schema.columns
where table_schema = 'silver'
and table_name = 'erp_cust_az12'
order by ordinal_position;




select *
from silver.crm_cust_info

-- Check for  duplicates
select cid, count(*) as conteo
from bronze.erp_cust_az12
group by cid
having count(*) > 1

--Check for nulls
select bdate 
from bronze.erp_cust_az12
where bdate is null 

--Check for unwanted spaces
select gen
from bronze.erp_cust_az12
where trim(gen) != gen

-- check Consistency
select bdate
from bronze.erp_cust_az12
where bdate <= '1900-01-01' or bdate >= GETDATE()

-- check  for distinct

select distinct gen
from bronze.erp_cust_az12 -- se identifican distintos tipos de categorias

select distinct
	gen,
	case when upper(trim(gen))  in ('F', 'FEMALE') then 'Female'
	when upper(trim(gen)) In  ('M', 'MALE') then 'Male'
	else 'N/A' 
	end as gen
	from bronze.erp_cust_az12

-- ============================================
-- Query Cleaning, Transformation and Insert
-- ============================================

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
	