/*
===============================================================================
Quality checks
===============================================================================
script purpose :
This script performs quality checks to validate the integrity ,consistency,
and accuracy of the gold layer .These checks ensure:
--uniqueness of surrogate keys inj dimensions tables.
--Referential integrity b/w fact and dimension tables.
--validation of relationship in the data model for analytical purpose.
Usage Notes:
-Investigate and resolve any discrepancies found during the checks.
===============================================================================
*/
--===============================================================================
--checking 'gold.dim_customers'
--===============================================================================
--check for uniqueness of customers key in gold.dim_customerd
--expectation : No results
select 
customer_key ,
count(*) as duplicate_count
from gold.dim_customers
group by customer_key
having count(*) >1;

--===============================================================================
--checking 'gold.product_key
--===============================================================================
--check for uniqueness of product key in gold.dim_products
--exceptation: no results
select 
product_key,
count(*) as duplicate_count
from gold.dim_products
group by product_key
having count(*) >1

--===============================================================================
--checking 'gold.fact_sales'
--===============================================================================
--cheeck the data model connectivity b/w fact and dimensions
select *
from gold.fact_sales f
left join gold.dim_customers c
on c.customer_key =f.customer_key
left join gold.dim_products p
on p.product_key=f.product_key
where p.product_key is null or c.customer_key is null
