/*
===============================================================================
CREATE DATABASE AND SCHEMAS 
===============================================================================
Script purpose: 
This script creates a new database named 'DataWarehouse' after cheacking for its existence.
If database already exists, it is dropped first. After creating the database, 
it switches to the new database context and creates three schemas: 'bronze', 'silver', and 'gold'.

Warning:
	Running this script will result in the loss of all existing data in the 'DataWarehouse' database if it already exists.
	All data will be permanently deleted.Proceed with caution.Ensure you have backups of any important data before executing this script.
*/

use master;
GO


-- Drop the database if it already exists
IF EXISTS (SELECT name FROM sys.databases WHERE name = 'DataWarehouse')
BEGIN
	ALTER DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
	DROP DATABASE DataWarehouse;
END;
Go

-- Create the DataWarehouse database
Create Database DataWarehouse;
GO

-- Switch to the DataWarehouse database context
use DataWarehouse;
GO

-- Create the bronze, silver, and gold schemas
GO
CREATE SCHEMA bronze;
GO
CREATE SCHEMA silver;
GO
CREATE SCHEMA gold;
GO
