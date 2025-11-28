/*
 =============================================
  📁 Initial Database
  🏷️ Create Database and Schemas
=============================================

Script Purpose:
This script creates a new database named 'DataWarehouse' .
It first checks whether the database already exists.
If the database exists, it will be DROPPED and RECREATED.
Additionally, the script sets up three schemas within the database:
   • bronze
   • silver
   • gold

⚠️ WARNING : 
Running this script will DROP the entire DataWarehouse database if it exists.
ALL DATA in the database will be permanently DELETED.
Proceed with caution and ensure you have a full backup before running this script.
*/
--create Database 'DataWhereHouse'

use master;
go 
  
--drop database 'DataWarehouse' if its exists
	if Exists(select 1 from sys.databases where name = 'DataWarehouse')
		begin 
			alter database DataWhereHouse set SINGEL_USER with rollback Immediate;
			drop database DataWarehouse;
		end;

--create the 'DataWarehouse' database
create database DataWarehouse;
Go

use DataWarehouse;
Go

--create Schema 

create Schema bronze;
Go
create Schema silver;
Go
create Schema gold;
Go
