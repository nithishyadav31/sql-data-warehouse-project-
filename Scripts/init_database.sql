/* 
=======================================================
Create database and schemas 
=======================================================
script purpose :
  This script creates a new database name "Data Warehouse" after checking if it already exists.
  If the databade exists,it is dropped and recreated.Additionally, the script aets up three schemas 
  within the database :'bronze','silver', and 'gold'.
warning:
  Running this scriot will drop the entire 'datawarehouse'database if it exists.
  all data in the database will be permanently deleted.proceed with caution and ensure you have proper backup option befor rinning the script.

  */

use master;
GO
--Drop and recreate the 'datawarehouse' database 
if exists (select 1 from sys.database where name='Datawarehouse')
  begin 
  alter database datawarehouse set single_user with rollback immediate;
drop database datawarehouse;
end;
go
create database Datawarehouse;
go
use Datawarehouse;

create schema bronze;
go 
create schema silver;
go
create schema gold;
go
