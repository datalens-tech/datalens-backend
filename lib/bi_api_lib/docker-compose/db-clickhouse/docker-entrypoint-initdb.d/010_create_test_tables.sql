CREATE DATABASE IF NOT EXISTS samples;
DROP TABLE IF EXISTS samples.SampleLite;
CREATE TABLE samples.SampleLite (Category String) ENGINE = MergeTree() ORDER BY tuple();
DROP TABLE IF EXISTS samples.SampleLiteCopy;
CREATE TABLE samples.SampleLiteCopy (Category String) ENGINE = MergeTree() ORDER BY tuple();

INSERT INTO samples.SampleLite VALUES ('A'), ('B'), ('C');


CREATE DATABASE IF NOT EXISTS test_data;

DROP TABLE IF EXISTS test_data.sample_superstore;
CREATE TABLE IF NOT EXISTS test_data.sample_superstore (
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

DROP TABLE IF EXISTS test_data.sample_superstore_hashed_customer_name;
CREATE TABLE IF NOT EXISTS test_data.sample_superstore_hashed_customer_name (
    Category String,
    City String,
    Country String,
    `Customer ID Renamed` String,
    `Customer Name` String,
    `Customer Name (hashed)` String,
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

DROP TABLE IF EXISTS test_data.ontime;
CREATE TABLE IF NOT EXISTS test_data.ontime (
    OriginStateFips String,
    Whatever String
) ENGINE = MergeTree() ORDER BY `OriginStateFips`;
INSERT INTO test_data.ontime (OriginStateFips, Whatever) VALUES ('qwe', 'rty');
INSERT INTO test_data.ontime (OriginStateFips, Whatever) VALUES ('asd', 'fgh');

DROP TABLE IF EXISTS test_data.null_date;
CREATE TABLE IF NOT EXISTS test_data.null_date (
    id Int64,
    date_value Date
) ENGINE = MergeTree() ORDER BY `id`;
INSERT INTO test_data.null_date (`id`, `date_value`) VALUES (1, '0000-00-00');
INSERT INTO test_data.null_date (`id`, `date_value`) VALUES (2, '0000-00-00');

DROP TABLE IF EXISTS test_data.SampleLite;
CREATE TABLE test_data.SampleLite (
    Category String,
    `Customer ID` String,
    Date Date,
    Month UInt8,
    Year UInt16,
    MontID UInt64,
    `Order ID` String,
    `Postal Code` Int32,
    Profit Float32,
    Region String,
    Sales Float32,
    Segment String,
    `Sub-Category` String
) ENGINE = MergeTree() ORDER BY tuple() SETTINGS index_granularity = 8192;

DROP TABLE IF EXISTS test_data.t_uuid;
CREATE TABLE IF NOT EXISTS test_data.t_uuid (x UUID, y String)
ENGINE = MergeTree() ORDER BY `y`;
INSERT INTO test_data.t_uuid (y) VALUES ('Example 0');
INSERT INTO test_data.t_uuid SELECT generateUUIDv4(), 'Example 1';
INSERT INTO test_data.t_uuid SELECT generateUUIDv4(), 'Example 2';

DROP TABLE IF EXISTS test_data.t_arrays;
CREATE TABLE IF NOT EXISTS test_data.t_arrays(
    descr String,
    arr_int Array(Nullable(Int32)),
    arr_float Array(Nullable(Float32)),
    arr_str Array(Nullable(String))
) ENGINE = MergeTree() ORDER BY tuple();
INSERT INTO test_data.t_arrays (descr, arr_int, arr_float, arr_str)
VALUES ('row 1', [0, 5, 33], [1.0, 0.12334, 20000.9], ['q', 'qwe', 'qwe qwe']);
INSERT INTO test_data.t_arrays (descr, arr_int, arr_float, arr_str)
VALUES ('row 2', [1, 15, 235], [1.0, 15.0, 235.0], ['1', '15', '235']);
INSERT INTO test_data.t_arrays (descr, arr_int, arr_float, arr_str)
VALUES ('empty array', [], [], []);
INSERT INTO test_data.t_arrays (descr, arr_int, arr_float, arr_str)
VALUES ('nulls array', [1, NULL, 0], [1.0, NULL, 0], ['1', NULL, '', 'qwerty']);


CREATE DATABASE IF NOT EXISTS other_test_data;

DROP TABLE IF EXISTS other_test_data.sample_superstore;
CREATE TABLE IF NOT EXISTS other_test_data.sample_superstore (
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
