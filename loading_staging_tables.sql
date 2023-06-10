-- LOADING DATA TO POSTGRESQL

-- NEW CARS DATA
CREATE TABLE staging_new_car (
  new_carid  SERIAL PRIMARY KEY,
  old_price NUMERIC,
  new_price NUMERIC,
  date_range TIMESTAMP,
  manufacturer VARCHAR(255),
  model VARCHAR(255),
  prod_year NUMERIC,
  gear_box_type VARCHAR(255),
  price_variation VARCHAR(255),
  price_change NUMERIC(10, 2)
);


COPY staging_new_car (old_price, new_price, date_range, manufacturer, model, prod_year, gear_box_type, price_variation, price_change)
FROM '/private/tmp/new_cars_df_cleaned (1).csv' DELIMITER ',' CSV HEADER;

SELECT * FROM staging_new_car


-- OLD CARS DATASET
CREATE TABLE staging_old_car (
  old_carid  SERIAL PRIMARY KEY,
  Month_Year varchar(255),
  Average_Price NUMERIC(10, 2),
  Minimum_Price NUMERIC(10, 2),
  Maximum_Price NUMERIC(10, 2),
  Manufacturer VARCHAR(255),
  Model VARCHAR(255),
  ProdYear VARCHAR
);
COPY staging_old_car 
FROM '/private/tmp/old_cars_df_cleaned.csv' DELIMITER ',' 
CSV HEADER;
SELECT * FROM staging_old_car

-- CAR Features DATASET
CREATE TABLE staging_car (
  car_priceid SERIAL PRIMARY KEY,
  Manufacturer VARCHAR(255),
  Model VARCHAR(255),
  ProdYear VARCHAR,
  Category VARCHAR(255),
  Leather_interior VARCHAR,
  Fuel_type VARCHAR(255),
  Engine_volume NUMERIC(10,2),
  Mileage INTEGER,
  Cylinders INTEGER,
  Gear_box_type VARCHAR(255),
  Color VARCHAR(255),
  Airbags INTEGER
);

COPY staging_car 
FROM '/private/tmp/cars_df_cleaned (1).csv' DELIMITER ',' 
CSV HEADER;
SELECT * FROM staging_car


--Data preprocessing using sql
-- removing white spaces from sql tables
CREATE OR REPLACE FUNCTION remove_whitespace_from_staging_new_car()
RETURNS VOID AS $$
BEGIN
  UPDATE staging_new_car
  SET 
    manufacturer = TRIM(manufacturer),
    model = TRIM(model),
    gear_box_type = TRIM(gear_box_type),
    price_variation = TRIM(price_variation);
END;
$$ LANGUAGE plpgsql;
CALL remove_whitespace_from_staging_new_car();



CREATE OR REPLACE FUNCTION remove_whitespace_from_staging_old_car()
RETURNS VOID AS $$
BEGIN
  UPDATE staging_old_car
  SET 
    Month_Year = TRIM(Month_Year),
    Manufacturer = TRIM(Manufacturer),
    Model = TRIM(Model),
    ProdYear = TRIM(ProdYear);
END;
$$ LANGUAGE plpgsql;
CALL remove_whitespace_from_staging_old_car();



CREATE OR REPLACE FUNCTION remove_whitespace_from_staging_car()
RETURNS VOID AS $$
BEGIN
  UPDATE staging_car
  SET 
    Manufacturer = TRIM(Manufacturer),
    Model = TRIM(Model),
    ProdYear = TRIM(ProdYear),
    Category = TRIM(Category),
    Leather_interior = TRIM(Leather_interior),
    Fuel_type = TRIM(Fuel_type),
    Gear_box_type = TRIM(Gear_box_type),
    Color = TRIM(Color);
END;
$$ LANGUAGE plpgsql;
CALL remove_whitespace_from_staging_car();



-- DELETING DUPLICATE VALUES FROM THE STAGING TABLES

-- FROM staging_new_car
DELETE FROM staging_new_car
WHERE (old_price, new_price, date_range,manufacturer, model, prod_year, gear_box_type, price_variation, price_change) IN (
  SELECT old_price, new_price, date_range,manufacturer, model, prod_year, gear_box_type, price_variation, price_change
  FROM new_car_prices
  GROUP BY  old_price, new_price, date_range,manufacturer, model, prod_year, gear_box_type, price_variation, price_change
  HAVING COUNT(*) > 1
);
select * from staging_new_car
where old_price=315000.0 and new_price=345000.0 and date_range='2022-04-07 00:00:00' and manufacturer='Hyundai' and model='Accent HCI' and prod_year=2022 

-- From old_car_prices
	
	DELETE FROM old_car_prices
WHERE (Month_Year,	Average_Price,	Minimum_Price,	Maximum_price,	Manufacturer,	Model,	ProdYear) IN (
SELECT Month_Year,	Average_Price,	Minimum_Price,	Maximum_price,	  Manufacturer,	Model,	ProdYear
  	FROM old_car_prices
GROUP BY  Month_Year,	Average_Price,	Minimum_Price,	Maximum_price,	Manufacturer,	Model,	ProdYear
 	 HAVING COUNT(*) > 1
);
select * from old_car_prices

-- staging_car table 
SELECT id, manufacturer, model,prodyear, category, leather_interior, fuel_type, engine_volume, mileage, color, airbags
  FROM Car_prices
  GROUP BY id, manufacturer, model,prodyear, category, leather_interior, fuel_type, engine_volume, mileage, color, airbags
  HAVING COUNT(*) > 1

