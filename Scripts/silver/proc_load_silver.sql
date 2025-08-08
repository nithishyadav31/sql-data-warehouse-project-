/*
=========================================================================================
Stored procedure : Load silver layer (bronze -> silver)
=========================================================================================
script purpose:
This stored procedure performs the ETL (extract,transform,load) process to 
populate the 'silver' schema tables from the 'bronze' schema.
action required:
-truncates silver tables.
-inserts transformed and cleaned data from bronze into silver tables.

parameters:
none
this stored procedure does not accept any parameters or return any values.
usage example:
EXEC silver.load_silver;
=========================================================================================
*/

create or alter procedure silver.load_silver as
begin
	declare @start_time datetime,@end_time datetime,@batch_start_time datetime ,@batch_end_time datetime;
	begin try
	set @batch_start_time =GETDATE();
	print'=============================';
	print'Loading Silver layer';
	print'=============================';
	print'=============================';
	print'Loading CRM Tables';
	print'=============================';
	set @start_time=getdate();
	print'>> Truncating table:silver.crm_cust_info'
	truncate table silver.crm_cust_info;
	print'>> Inserting data into :silver.crm_cust_info'
	insert  into silver.crm_cust_info(
	cst_id ,
	cst_key	,
	cst_firstname ,
	cst_lastname  ,	
	cst_marital_status	,
	cst_gndr, 
	cst_create_date
	)
	select 
	cst_id,
	cst_key,
	trim(cst_firstname) as cst_firstname,
	trim(cst_lastname) as cst_lastname,
	case when upper(trim(cst_marital_status))='M' then 'Married'
	when upper(trim(cst_marital_status))='S' then 'Single'
	else 
	'n/a'
	end cst_marital_status,
	case when upper(trim(cst_gndr))='M' then 'Male'
	when upper(trim(cst_gndr))='F' then 'Female'
	else 
	'n/a'
	end cst_gndr,
	cst_create_date
	from(
		select *,
		ROW_NUMBER() over(partition by cst_id order by cst_create_date desc) as flag_last
		from bronze.crm_cust_info
		where cst_id is not null
	)t where flag_last=1
	set @end_time =getdate();
	print'load duration :' + cast(datediff(second,@start_time,@end_time) as nvarchar(50))+ 'seconds';
	print'----------------------------------';
	set @start_time=getdate(); 
	print'>> Truncating table:silver.crm_prd_info'
	truncate table silver.crm_prd_info;
	print'>> Inserting data into :silver.crm_prd_info'
	insert into silver.crm_prd_info(
	prd_id ,
	cat_id,
	prd_key	,
	prd_nm 	,
	prd_cost ,
	prd_line ,
	prd_start_dt ,
	prd_end_dt 
	)
	select 
	prd_id,
	replace(substring(prd_key,1,5) ,'-','_')as cat_id, --extract category_id
	substring(prd_key,7,len(prd_key)) as prd_key, --extract product_id
	prd_nm,
	isnull(prd_cost,0) as prd_cost, --missing values
	case upper(trim(prd_line))
	when 'R' then 'Road'
	when 'M' then 'Mountain'
	when 'T' then 'Touring'
	when 'S' then 'other sales'
	else 'n/a' 
	end prd_line, --normalization
	cast(prd_start_dt as date)  as prd_start_dt,--casting
	cast(dateadd (day,-1,lead(prd_start_dt) over(partition by prd_key order by prd_start_dt) )as date)as prd_end_dt --data enrichment 
	from bronze.crm_prd_info;
	set @end_time =getdate();
	print'load duration:' + cast(datediff(second,@start_time,@end_time) as nvarchar(50)) + 'seconds'
	print'----------------------------------';
	set @start_time=getdate();
	print'>> Truncating table:silver.crm_sales_details'
	truncate table silver.crm_sales_details;
	print'>> Inserting data into :silver.crm_sales_details'
	insert into silver.crm_sales_details(
	sls_ord_num ,
	sls_prd_key,
	sls_cust_id ,
	sls_order_dt ,
	sls_ship_dt ,
	sls_due_dt ,
	sls_sales ,
	sls_quantity,
	sls_price 
	)
	select 
		sls_ord_num ,
		sls_prd_key ,
		sls_cust_id  ,
		case when sls_order_dt=0 or len(sls_order_dt) !=8 then null
		else cast(cast(sls_order_dt as varchar) as date )
		end sls_order_dt   ,
		case when  sls_ship_dt=0 or len( sls_ship_dt) !=8 then null
		else cast(cast( sls_ship_dt as varchar) as date )
		end sls_ship_dt,
		case when sls_due_dt=0 or len(sls_due_dt) !=8 then null
		else cast(cast(sls_due_dt as varchar) as date )  
		end as sls_due_dt   ,
		case when sls_sales is null or  sls_sales<=0 or  sls_sales!= abs(sls_price) *  sls_quantity
		then abs(sls_price) *  sls_quantity
		else sls_sales end as sls_sales,
		sls_quantity,
		case when sls_price is null or sls_price<=0
		then sls_sales /nullif(sls_quantity,0) 
		else sls_price
		end as sls_price
	from bronze.crm_sales_details
	set @end_time =getdate();
	print'load duration:' + cast(datediff(second,@start_time,@end_time) as nvarchar(50)) + 'seconds'
	print'----------------------------------';
	print'=============================';
	print'Loading ERP Tables';
	print'=============================';
	set @start_time=getdate();
	print'>> Truncating table:silver.erp_cust_az12'
	truncate table silver.erp_cust_az12;
	print'>> Inserting data into :silver.erp_cust_az12'
	insert into silver.erp_cust_az12(cid,BDATE,GEN)
	select 
		case when cid like 'NAS%' then substring(cid,4,len(cid)) 
		else cid 
		end
		as cid,--Removed 'NAS' prefix if present in cid
		case when BDATE > getdate() then null
		else BDATE
		end as BDATE,--set future birthday to nulls
		case when upper(trim(gen)) in ('F','female') then 'Female'
		when upper(trim(gen)) in ('M','Male') then 'Male'
		else 'n/a'  
		end as gen --normalized the gender handle the cases
	from bronze.erp_cust_az12
	set @end_time =getdate();
	print'load duration:' + cast(datediff(second,@start_time,@end_time) as nvarchar(50)) + 'seconds'
	print'----------------------------------';
	set @start_time=getdate();
	print'>> Truncating table:silver.erp_loc_a101'
	truncate table silver.erp_loc_a101;
	print'>> Inserting data into :silver.loc_a101'
	INSERT INTO silver.erp_loc_a101(CID,CNTRY)
	select 
	replace(cid,'-','') as cid,
	case
	when trim(cntry)='' or cntry is null then 'n/a'
	when trim(cntry)='de' then'Germany'
	when trim(cntry) in('us' , 'usa') then 'United States'
	else trim(cntry)end as cntry
	from bronze.erp_loc_a101;
	set @end_time =getdate();
	print'load duration:' + cast(datediff(second,@start_time,@end_time) as nvarchar(50)) + 'seconds'
	print'----------------------------------';
	set @start_time=getdate();
	print'>> Truncating table:silver.erp_px_cat_g1v2'
	truncate table silver.erp_px_cat_g1v2;
	print'>> Inserting data into :silver.erp_px_cat_g1v2'
	insert into silver.erp_px_cat_g1v2(id,cat,subcat,maintenance)
	SELECT 
	id,
	cat,
	subcat,
	maintenance
	FROM bronze.erp_px_cat_g1v2;
	set @end_time =getdate();
	print'load duration:' + cast(datediff(second,@start_time,@end_time) as nvarchar(50)) + 'seconds';
	print'----------------------------------';
	set @batch_end_time =GETDATE();
	print'batch duration :' +cast(datediff(second,@batch_start_time,@batch_end_time ) as nvarchar(50)) + 'seconds';
	print'=============================';
	print'bronze layer  is completed';
	print'=============================';
	end try 
	begin catch
	print'==========================';
	print 'error occured during silver layer execution';
	print'error message'+error_message();
	print'error number'+cast(error_number() as nvarchar(50));
	print'error state'+cast(error_state()as nvarchar(50));
	end catch
end

