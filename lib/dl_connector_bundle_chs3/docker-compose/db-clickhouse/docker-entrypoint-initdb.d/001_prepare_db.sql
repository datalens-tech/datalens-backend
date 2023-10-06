CREATE DATABASE IF NOT EXISTS test_data;

DROP TABLE IF EXISTS test_data.sample;
CREATE TABLE IF NOT EXISTS test_data.sample (
    Category String,
    City String,
    Country String,
    `Customer ID` String,
    `Customer Name` String,
    Discount Float32,
    `Order Date` Date,
    `Order ID` String,
    `Postal Code` Int32,
    `Product ID` String,
    `Product Name` String,
    Profit Float32,
    Quantity Int32,
    Region String,
    `Row ID` Int32,
    Sales Float32,
    Segment String,
    `Ship Date` Date,
    `Ship Mode` String,
    State String,
    `Sub-Category` String
) ENGINE = MergeTree() ORDER BY `Order Date`;
