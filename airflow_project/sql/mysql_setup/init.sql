-- This script is executed when the MySQL container starts for the first time.
-- The database and user specified by MYSQL_DATABASE, MYSQL_USER, MYSQL_PASSWORD
-- are automatically created by the MySQL Docker image.
-- This script can be used to create tables, schemas, or other initial setup.

-- Make sure we are using the correct database (as specified by MYSQL_DATABASE env var).
-- The database name is taken from the .env file, e.g., 'staging_db'
USE `staging_db`;

-- Create the raw_flight_data table if it doesn't already exist.
-- This table will store the raw data ingested from the CSV.
CREATE TABLE IF NOT EXISTS `raw_flight_data` (
    `Airline` VARCHAR(255),
    `Source` VARCHAR(10),
    `Source Name` VARCHAR(255),
    `Destination` VARCHAR(10),
    `Destination Name` VARCHAR(255),
    `Departure Date & Time` DATETIME,
    `Arrival Date & Time` DATETIME,
    `Duration (hrs)` DECIMAL(10,5),
    `Stopovers` VARCHAR(50),
    `Aircraft Type` VARCHAR(100),
    `Class` VARCHAR(50),
    `Booking Source` VARCHAR(100),
    `Base Fare (BDT)` DECIMAL(15,2),
    `Tax & Surcharge (BDT)` DECIMAL(15,2),
    `Total Fare (BDT)` DECIMAL(15,2),
    `Seasonality` VARCHAR(50),
    `Days Before Departure` INT,
    INDEX idx_airline (`Airline`),
    INDEX idx_source_dest (`Source`, `Destination`),
    INDEX idx_departure_time (`Departure Date & Time`)
);
