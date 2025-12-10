/*
=============================================================
Create Database and Schemas
=============================================================
Script Description:
    This script creates a fresh database called 'DataWarehouse' after first 
    checking whether it already exists. If it does, the database is dropped 
    and then created again. It also defines three schemas in that database: 
    'bronze', 'silver', and 'gold'.

Notes:
    - This script is intended to be executed via psql, because PostgreSQL does
      not support the USE command like SQL Server or MySQL. Switching between
      databases is done by opening a new connection, not by an SQL command.
    - In pgAdmin, multiple commands executed together are often wrapped in a
      transaction, which can cause the error
      "DROP DATABASE cannot run inside a transaction block" when DROP DATABASE
      is included.
*/

DROP DATABASE IF EXISTS "DataWarehouse";
CREATE DATABASE "DataWarehouse";

\c "DataWarehouse"

CREATE SCHEMA bronze;
CREATE SCHEMA silver;
CREATE SCHEMA gold;
