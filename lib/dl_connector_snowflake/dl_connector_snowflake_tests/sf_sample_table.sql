use SAMPLE_DB;
use schema SAMPLE_SCHEMA;
use warehouse PY_TEST;

-- DROP TABLE IF EXISTS "SAMPLE_TABLE";

CREATE TABLE IF NOT EXISTS "SAMPLE_TABLE" (
    "Category" VARCHAR(255),
    "City" VARCHAR(255),
    "Country" VARCHAR(255),
    "Customer ID" VARCHAR(255),
    "Customer Name" VARCHAR(255),
    "Discount" NUMERIC(16, 8),
    "Order Date" DATE,
    "Order ID" VARCHAR(255),
    "Postal Code" INTEGER,
    "Product ID" VARCHAR(255),
    "Product Name" VARCHAR(255),
    "Profit" NUMERIC(16, 8),
    "Quantity" INTEGER,
    "Region" VARCHAR(255),
    "Row ID" INTEGER PRIMARY KEY,
    "Sales" NUMERIC(16, 8),
    "Segment" VARCHAR(255),
    "Ship Date" DATE,
    "Ship Mode" VARCHAR(255),
    "State" VARCHAR(255),
    "Sub-Category" VARCHAR(255)
);


