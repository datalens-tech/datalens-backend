DROP TABLE IF EXISTS rates;
CREATE TABLE rates (
    `Category` VARCHAR(255),
    `City` VARCHAR(255),
    `Country` VARCHAR(255),
    `Customer ID` VARCHAR(255),
    `Customer Name` VARCHAR(255),
    `Discount` FLOAT,
    `Order Date` DATE,
    `Order ID` VARCHAR(255),
    `Postal Code` INTEGER,
    `Product ID` VARCHAR(255),
    `Product Name` VARCHAR(255),
    `Profit` FLOAT,
    `Quantity` INTEGER,
    `Region` VARCHAR(255),
    `Row ID` INTEGER PRIMARY KEY,
    `Sales` FLOAT,
    `Segment` VARCHAR(255),
    `Ship Date` DATE,
    `Ship Mode` VARCHAR(255),
    `State` VARCHAR(255),
    `Sub-Category` VARCHAR(255),
    INDEX order_date_idx (`Order Date`)
) ENGINE=InnoDB;


LOAD DATA LOCAL INFILE '/common-data/sample.csv'
INTO TABLE rates FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"' LINES TERMINATED BY '\n' IGNORE 0 ROWS
(
    `Category`, `City`, `Country`, `Customer ID`, `Customer Name`, `Discount`, `Order Date`, `Order ID`,
    `Postal Code`, `Product ID`, `Product Name`, `Profit`, `Quantity`, `Region`, `Row ID`, `Sales`, `Segment`,
    `Ship Date`, `Ship Mode`, `State`, `Sub-Category`
);