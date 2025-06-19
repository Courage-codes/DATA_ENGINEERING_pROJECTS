-- Create the processed_flight_data table if it doesn't already exist.
CREATE TABLE IF NOT EXISTS "processed_flight_data" (
    "Airline" VARCHAR(255),
    "Source" VARCHAR(10),
    "Destination" VARCHAR(10),
    "Departure Date & Time" TIMESTAMP WITHOUT TIME ZONE,
    "Base Fare (BDT)" NUMERIC(15,2),
    "Tax & Surcharge (BDT)" NUMERIC(15,2),
    "Total Fare (BDT)" NUMERIC(15,2),
    "Month" INTEGER,
    "Day" INTEGER,
    "Seasonality_Label" VARCHAR(50),
    "Route" VARCHAR(50),
    CONSTRAINT chk_month CHECK ("Month" >= 1 AND "Month" <= 12),
    CONSTRAINT chk_day CHECK ("Day" >= 1 AND "Day" <= 31),
    CONSTRAINT processed_flight_data_pk PRIMARY KEY ("Airline", "Source", "Destination", "Departure Date & Time")
);

CREATE INDEX IF NOT EXISTS idx_pfd_airline ON "processed_flight_data"("Airline");
CREATE INDEX IF NOT EXISTS idx_pfd_route ON "processed_flight_data"("Route");
CREATE INDEX IF NOT EXISTS idx_pfd_seasonality ON "processed_flight_data"("Seasonality_Label");
CREATE INDEX IF NOT EXISTS idx_pfd_departure_time ON "processed_flight_data"("Departure Date & Time");

-- KPI tables remain unchanged
CREATE TABLE IF NOT EXISTS "kpi_avg_fare_by_airline" (
    "Airline" VARCHAR(255) PRIMARY KEY,
    "Total Fare (BDT)" NUMERIC(15,2)
);

CREATE TABLE IF NOT EXISTS "kpi_seasonal_fare_variation" (
    "Seasonality_Label" VARCHAR(50) PRIMARY KEY,
    "Total Fare (BDT)" NUMERIC(15,2)
);

CREATE TABLE IF NOT EXISTS "kpi_booking_count_by_airline" (
    "Airline" VARCHAR(255) PRIMARY KEY,
    "booking_count" INTEGER
);

CREATE TABLE IF NOT EXISTS "kpi_popular_routes" (
    "Route" VARCHAR(50) PRIMARY KEY,
    "booking_count" INTEGER
);
