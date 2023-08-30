DROP TABLE IF EXISTS sample;
CREATE TABLE IF NOT EXISTS sample (
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
CREATE INDEX s_order_date_idx ON sample ("Order Date");


COPY sample (
    "Category", "City", "Country", "Customer ID", "Customer Name", "Discount", "Order Date", "Order ID",
    "Postal Code", "Product ID", "Product Name", "Profit", "Quantity", "Region", "Row ID", "Sales", "Segment",
    "Ship Date", "Ship Mode", "State", "Sub-Category"
)
FROM '/common-data/sample.csv' WITH (FORMAT csv, DELIMITER ',', QUOTE '"');


DROP TABLE IF EXISTS supersample;
CREATE TABLE IF NOT EXISTS supersample (
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
CREATE INDEX ss_order_date_idx ON supersample ("Order Date");


COPY supersample (
    "Category", "City", "Country", "Customer ID", "Customer Name", "Discount", "Order Date", "Order ID",
    "Postal Code", "Product ID", "Product Name", "Profit", "Quantity", "Region", "Row ID", "Sales", "Segment",
    "Ship Date", "Ship Mode", "State", "Sub-Category"
)
FROM '/common-data/sample.csv' WITH (FORMAT csv, DELIMITER ',', QUOTE '"');
