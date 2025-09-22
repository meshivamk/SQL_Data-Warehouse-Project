use master;	/*	We must be in master to create a database (if we are using drop statements like below), 
				cant create a database inside another database, cant drop a database while using that database */

go
			/*	It works as a batch sperator and treats the statement above it as a batch and executes them, 
				The statements between two Go are treate seperately. 
				GO n, executes the batch above it n times.
				eg: GO 3, executes the batch 3 times */
		
drop database if exists Datawarehouse;
go

create database Datawarehouse;
go

use DataWarehouse;
go 

create schema bronze; /*IF SCHEMA_ID('bronze') IS NULL
							EXEC('CREATE SCHEMA bronze;'); --- use this if creating in an already existing database */
GO

go
create schema silver;
go
create schema gold;
go