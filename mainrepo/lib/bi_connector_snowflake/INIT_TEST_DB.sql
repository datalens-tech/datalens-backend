create role if not exists DL_TEST_ROLE;

--  granting users with the SYSADMIN role to modify the DL_TEST_ROLE
grant role DL_TEST_ROLE to role SYSADMIN;

create warehouse if not exists DL_TEST_WH
    warehouse_size = 'x-small'
    warehouse_type = standard
    auto_suspend = 120
    auto_resume = true
    initially_suspended = true;

-- set limit
CREATE OR REPLACE RESOURCE MONITOR DL_TEST_WH_LIMIT_DAILY WITH CREDIT_QUOTA = 10
    FREQUENCY = DAILY START_TIMESTAMP = IMMEDIATELY;
CREATE OR REPLACE RESOURCE MONITOR WH_LIMIT_MONTHLY WITH CREDIT_QUOTA = 50
    FREQUENCY = MONTHLY START_TIMESTAMP = IMMEDIATELY;
ALTER WAREHOUSE SET RESOURCE_MONITOR = DL_TEST_WH_LIMIT_DAILY;

grant all privileges
    on warehouse DL_TEST_WH
    to role DL_TEST_ROLE;

-- create user
create user if not exists DL_TEST_USER_X password = provide a password here;
grant role DL_TEST_ROLE to user DL_TEST_USER_X;
alter user DL_TEST_USER_X set default_role = DL_TEST_ROLE default_warehouse = 'DL_TEST_WH';

CREATE DATABASE IF NOT EXISTS DL_TEST_DB_X;
GRANT OWNERSHIP ON DATABASE DL_TEST_DB_X TO ROLE DL_TEST_ROLE;

-- login into snowflake as DL_TEST_USER_X
-- snowsql -a "xxxxxxxx.eu-central-1" -u DL_TEST_USER_X
use DATABASE DL_TEST_DB_X;
CREATE SCHEMA if not exists DL_TEST_SCHEMA_X
use SCHEMA DL_TEST_SCHEMA_X;
use WAREHOUSE DL_TEST_WH;

CREATE TABLE IF NOT EXISTS "SAMPLE_TABLE"
(
    "Category"      VARCHAR(255),
    "City"          VARCHAR(255),
    "Country"       VARCHAR(255),
    "Customer ID"   VARCHAR(255),
    "Customer Name" VARCHAR(255),
    "Discount"      NUMERIC(16, 8),
    "Order Date"    DATE,
    "Order ID"      VARCHAR(255),
    "Postal Code"   INTEGER,
    "Product ID"    VARCHAR(255),
    "Product Name"  VARCHAR(255),
    "Profit"        NUMERIC(16, 8),
    "Quantity"      INTEGER,
    "Region"        VARCHAR(255),
    "Row ID"        INTEGER PRIMARY KEY,
    "Sales"         NUMERIC(16, 8),
    "Segment"       VARCHAR(255),
    "Ship Date"     DATE,
    "Ship Mode"     VARCHAR(255),
    "State"         VARCHAR(255),
    "Sub-Category"  VARCHAR(255)
);

CREATE OR REPLACE FILE FORMAT mycsvformat TYPE = 'CSV' FIELD_DELIMITER = ',' FIELD_OPTIONALLY_ENCLOSED_BY = '"';
CREATE OR REPLACE STAGE my_csv_stage FILE_FORMAT = mycsvformat;
-- fix path
put sample.csv @my_csv_stage AUTO_COMPRESS = TRUE;
COPY INTO SAMPLE_TABLE FROM @my_csv_stage/sample.csv.gz FILE_FORMAT = (FORMAT_NAME = mycsvformat) ON_ERROR = 'skip_file';
