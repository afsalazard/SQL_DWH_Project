
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
-- Check for duplicates or null in primary key
-- ============================================
use  data_warehouse
select cst_id, count(*) 
from bronze.crm_cust_info
group by cst_id
having COUNT(*)  > 1 or cst_id is null


select*
from(
select * , row_number() over (partition by cst_id order by  cst_create_date desc) as flag_last
from bronze.crm_cust_info)
t where flag_last != 1 and  cst_id = 29449


-- ============================================
-- Check for unwanted spaces
-- ============================================
select cst_firstname
from bronze.crm_cust_info 
where cst_firstname != trim(cst_firstname)


-- Count unwanted spaces

select count(*)
from (
select cst_firstname
from bronze.crm_cust_info 
where cst_firstname != trim(cst_firstname)) t

-- Filter  table 

select * from bronze.crm_cust_info

select cst_id, cst_key, trim(cst_firstname) as cst_fisrt_name, trim(cst_lastname) as cst_last_name, cst_marital_status,	cst_gndr, cst_create_date
from bronze.crm_cust_info

-- ============================================
--data standardization & consistency
-- ============================================

select distinct cst_gndr
from bronze.crm_cust_info

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

from bronze.crm_cust_info;

-- ============================================
--Load into Silver Layer
-- ============================================
	truncate table silver.crm_cust_info;  	-- elimina registros antes de insertar
	
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


select cst_id, count(*)
from silver.crm_cust_info
group by cst_id
having count(*) > 1


select cst_lastname
from silver.crm_cust_info 
where cst_lastname != trim(cst_lastname)

select *
from silver.crm_cust_info
