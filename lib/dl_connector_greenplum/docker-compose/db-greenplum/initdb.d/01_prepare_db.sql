CREATE SCHEMA IF NOT EXISTS test_data;
DROP TABLE IF EXISTS test_data.sample;
CREATE TABLE test_data.sample (
    "Category" VARCHAR(255),
    "City" VARCHAR(255),
    "Country" VARCHAR(255),
    "Customer ID" VARCHAR(255),
    "Customer Name" VARCHAR(255),
    "Discount" FLOAT,
    "Order Date" DATE,
    "Order ID" VARCHAR(255),
    "Postal Code" INTEGER,
    "Product ID" VARCHAR(255),
    "Product Name" VARCHAR(255),
    "Profit" FLOAT,
    "Quantity" INTEGER,
    "Region" VARCHAR(255),
    "Row ID" INTEGER PRIMARY KEY,
    "Sales" FLOAT,
    "Segment" VARCHAR(255),
    "Ship Date" DATE,
    "Ship Mode" VARCHAR(255),
    "State" VARCHAR(255),
    "Sub-Category" VARCHAR(255)
);
create index order_date_idx on test_data.sample("Order Date");

COPY test_data.sample (
    "Category", "City", "Country", "Customer ID", "Customer Name", "Discount", "Order Date", "Order ID",
    "Postal Code", "Product ID", "Product Name", "Profit", "Quantity", "Region", "Row ID", "Sales", "Segment",
    "Ship Date", "Ship Mode", "State", "Sub-Category"
)
FROM '/common-data/sample.csv' WITH (FORMAT csv, DELIMITER ',', QUOTE '"');
