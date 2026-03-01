/* Este script reinicia la base de datos 'data_warehouse': la borra si ya existe, la crea desde cero y configura los esquemas necesarios para el modelo Medallion */
-- create datawarehouse

use master;
go

-- quitar y crear data warehouse database
if exists (select 1 from sys.databases where name = 'data_warehouse')
begin
	alter database data_warehouse set single_user  with rollback immediate;
	drop database data_warehouse;
end;
go

-- crear data_warehouse database

create database data_warehouse;
go
use data_warehouse;
go
--crear schemas medallion
create schema bronze;
go -- separador de lotes batches indica que termine de procesar lo anterior antes de continuar
create schema silver;
go
create schema gold;
go
