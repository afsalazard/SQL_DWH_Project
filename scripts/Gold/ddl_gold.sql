
/*===============================================================================
GOLD LAYER DDL - DIMENSIONAL MODEL (VIEWS)
--===============================================================================
Description:
    This script defines the Gold Layer (Final Consumption Layer) for Power BI.
    It transforms Silver Layer data into a Star Schema for analytical reporting.

Views Breakdown:
    1. gold.dim_customers: Master customer data (CRM + ERP + Location).
    2. gold.dim_products: Product catalog with category and subcategory info.
    3. gold.fact_sales: Sales transactions linked to customers and products.

Business Rules & Logic:
    - Naming Standard: Transition from source naming to clean Business Names.
    - Gender Logic: Prioritizes CRM data, falls back to ERP, defaults to 'N/A'.
    - Formatting: Capitalizes the first letter of the 'Country' field.
    - Product Filtering: Includes only active products (prd_end_dt IS NULL).
    - Surrogate Keys: Generated using ROW_NUMBER() for dimensional integrity.*/
--===============================
-- DDL Gold Layer
--===============================

-- Create Dimension Customers
go
create or alter view gold.dim_customers as 
select 
	row_number() over (order by cst_id) as customer_key,
	ci.cst_id as customer_id,
	ci.cst_key as customer_number,
	ci.cst_firstname as first_name,
	ci.cst_lastname as last_name,
	upper (left(la.cntry,1)) + lower(substring(la.cntry,2,len(la.cntry))) as country,
	ci.cst_marital_status as marital_status,
	case  when ci.cst_gndr != 'N/A' then ci.cst_gndr
	else	 coalesce(ca.gen,'N/A')
	end as gender,
	ca.bdate as birth_date,
	ci.cst_create_date as create_date
	
	
from silver.crm_cust_info ci
left join silver.erp_cust_az12 ca
on ci.cst_key = ca.cid
left join silver.erp_loc_a101 la
on ci.cst_key = la.cid

-- Create Dimension Products
go
create or alter view gold.dim_products as
select 
	row_number() over  (order by pd.prd_start_dt,pd.prd_key) as product_key, -- Generamos llave con base en la fecha inicial y el  prd_key
	pd.prd_id as product_id,
	pd.prd_key as product_number,
	pd.prd_nm as product_name,
	pd.cat_id as category_id,
	pc.cat as category,
	pc.subcat as subcategory,
	pc.maintenance,
	pd.prd_cost as product_cost,
	pd.prd_line as product_line,
	pd.prd_start_dt  as start_date

	from silver.crm_prd_info pd

	left join silver.erp_px_cat_g1v2 pc
	on pd.cat_id = pc.id
	where prd_end_dt is null -- filter of product current information

	-- Create Fact Sales
go
create or alter view gold.fact_sales as
	select 
		sd.sls_ord_num as order_number,
		pd.product_key,
		cu.customer_key,
		sd.sls_order_dt as order_date,
		sd.sls_ship_dt as ship_date,
		sd.sls_due_dt as due_date,
		sd.sls_sales as sales_amount,
		sd.sls_quantity as  quantity,
		sd.sls_price as price
	from silver.crm_sales_details sd
	left join gold.dim_customers cu
	on sd.sls_cust_id = cu.customer_id
	left join gold.dim_products pd
	on sd.sls_prd_key = pd.product_number
