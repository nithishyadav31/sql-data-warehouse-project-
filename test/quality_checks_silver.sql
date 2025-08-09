/* 
==============================================================================================
Quality Checks
==============================================================================================
Script Purpose:
This script performs various quality checks for data consistency ,accuracy, and 
standardization across the 'silver layer'.It includes checks for:
-Null or duplicate primary keys.
-unwanted spaces in string fields.
-data standardization and consistency .
-Invalid date range and orders.
-Data consistency between related fields.

usage notes:
-run these checks after loading silver layer.
-Investigate and resolve any discrepancies found during the checks.
==============================================================================================
*/

--==============================================================================================
-- Checking 'silver.crm_cust_info'
--==============================================================================================
--check for null or duplicate in pk
--exception : no results
select 
cst_id
count(*) 
from silver.crm_cust_info
group by cst_id
having count(*) >1 or cst_id is null;

--check unwanted spaces
--exception :no results
select 
cst_key
from silver.crm_cust_info
where cst_key!=trim(cst_key);

--Data standardization & consistency 
select distinct
cst_material_status
from silver.crm_cust_info;

--==============================================================================================
--checking 'silver.crm_prd_info
--==============================================================================================
--check for nulls or duplicates in pk
--exception : No results
select 
prd_id
count(*)
from silver.crm_prd_info
group ny prd_id
having count(*) > 1 or prd_id is null;

--check for nulls or negative values in cost
--exception : no results
select 
prd_cost
from silver.crm_prd_info
where prd_cost < 0 or prd_cost is null;

--data standardization & consistency
select distinct
prd_line
from silver.crm_prd_info;

--checking invalids dates 
--exception : no results
select *
from silver.crm_prd_info
where prd_end_dt < prd_start_dt;

--==============================================================================================
--checking 'silver.crm_sales_details
--==============================================================================================
--check for invalid dates
--exception :no invalid dates

select 
nullif(sls_due_dt,0) as sls_due_dt
from silver.crm_sales_details 
where sls_due_dt <=0
or len(sls_due_dt) !=8
or sls_due_dt > 20500101
or sls_due_dt < 19000101


-- Check for Invalid Date Orders (Order Date > Shipping/Due Dates)
-- Expectation: No Results
SELECT 
    * 
FROM silver.crm_sales_details
WHERE sls_order_dt > sls_ship_dt 
   OR sls_order_dt > sls_due_dt;

-- Check Data Consistency: Sales = Quantity * Price
-- Expectation: No Results
SELECT DISTINCT 
    sls_sales,
    sls_quantity,
    sls_price 
FROM silver.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price
   OR sls_sales IS NULL 
   OR sls_quantity IS NULL 
   OR sls_price IS NULL
   OR sls_sales <= 0 
   OR sls_quantity <= 0 
   OR sls_price <= 0
ORDER BY sls_sales, sls_quantity, sls_price;

-- ====================================================================
-- Checking 'silver.erp_cust_az12'
-- ====================================================================
-- Identify Out-of-Range Dates
-- Expectation: Birthdates between 1924-01-01 and Today
SELECT DISTINCT 
    bdate 
FROM silver.erp_cust_az12
WHERE bdate < '1924-01-01' 
   OR bdate > GETDATE();

-- Data Standardization & Consistency
SELECT DISTINCT 
    gen 
FROM silver.erp_cust_az12;

-- ====================================================================
-- Checking 'silver.erp_loc_a101'
-- ====================================================================
-- Data Standardization & Consistency
SELECT DISTINCT 
    cntry 
FROM silver.erp_loc_a101
ORDER BY cntry;

-- ====================================================================
-- Checking 'silver.erp_px_cat_g1v2'
-- ====================================================================
-- Check for Unwanted Spaces
-- Expectation: No Results
SELECT 
    * 
FROM silver.erp_px_cat_g1v2
WHERE cat != TRIM(cat) 
   OR subcat != TRIM(subcat) 
   OR maintenance != TRIM(maintenance);

-- Data Standardization & Consistency
SELECT DISTINCT 
    maintenance 
FROM silver.erp_px_cat_g1v2;
