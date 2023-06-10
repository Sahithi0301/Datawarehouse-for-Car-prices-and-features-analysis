-- Creation of dimensional schema
-- Creating dim_car
Create table dim_Car
(
	dim_carid SERIAL PRIMARY KEY,
	featureid int,
	manufacturer varchar(255),
	model varchar(255),
	engine_volume int,
	airbags int,
	interior varchar(255),
	color varchar(255),
	fuel_type varchar(255),
	current_flag boolean DEFAULT TRUE,
	effective_timestamp timestamp DEFAULT NOW(),
	expired_timestamp timestamp
)

INSERT INTO dim_car (
	featureid,
    manufacturer,
    model,
    engine_volume,
    airbags,
    interior,
    color,
    fuel_type
)
SELECT
	f.featureid,
    c.manufacturer,
    m.model_name,
    f.airbags,
    f.engine_volume,
    i.leather_interior,
    co.color,
    fu.fuel_type
FROM features f
JOIN car c ON c.carid = f.carid
JOIN model m ON m.modelid = f.modelid
JOIN interior i ON i.interiorid = f.interiorid
JOIN color co ON co.colorid = f.colorid
JOIN fuel fu ON fu.fuelid = f.fuelid;

select * from dim_car


CREATE table Dim_gear_box
(
	Dim_gear_boxID Serial Primary Key,
	gear_box_type varchar(255)
)

Insert into Dim_gear_box(gear_box_type)
Select Gear_box_type from Gear_box



CREATE table Dim_ProdYear
(
	Dim_ProdYearID Serial Primary Key,
	ProdYear varchar(255)
)
Insert into Dim_ProdYear(ProdYear)
Select prod_year from year


CREATE TABLE date_dim (
    date_id SERIAL PRIMARY KEY,
    date TIMESTAMP,
    day VARCHAR(10),
    month VARCHAR(10),
    year INT
);
INSERT INTO date_dim(date, day, month, year)
SELECT 
    date_range as date,
    TO_CHAR(date_range, 'Day') AS day,
    TO_CHAR(date_range, 'Month') AS month,
    EXTRACT(YEAR FROM date_range) AS year
FROM 
(
	select date_range  from new_car_prices
	union
	select date_range from old_car_prices
) as d



create table fact_car(
	
	dim_carID int,
	date_iD int,
	dim_ProdYeariD int,
	dim_gear_boxiD int,
	Foreign key(dim_carID) REFERENCES dim_car(dim_carID),
	Foreign key(date_iD)  REFERENCES date_dim(date_iD),
	Foreign key(dim_ProdYeariD)  REFERENCES dim_ProdYear(dim_ProdYeariD),
	Foreign key(dim_gear_boxiD)  REFERENCES dim_gear_box(dim_gear_boxiD),
	max_airbags int,
	avg_enginevolume int,
	leather_interior_count int,
	popular_color int,
	popularfuel_type int,
	high_pricechange int,
	low_pricechange int,
	Highest_price int,
	lowest_price int
)

INSERT INTO fact_car (
    dim_carID,
    date_iD,
    dim_ProdYeariD,
    dim_gear_boxiD,
    max_airbags,
    avg_enginevolume,
    leather_interior_count,
    high_pricechange,
    low_pricechange,
    Highest_price,
    lowest_price
)
SELECT
    dc.dim_carID,
    dd.date_id AS dim_dateiD,
    y.yearid AS dim_ProdYeariD,
    dgb.dim_gear_boxid AS dim_gear_boxiD,
    MAX(dc.airbags) AS max_airbags,
    AVG(dc.engine_volume) AS avg_enginevolume,
    SUM(CASE WHEN dc.interior = 'Yes' THEN 1 ELSE 0 END) AS leather_interior_count,
    MAX(ncp.price_change) AS high_priceVariation,
    MIN(ncp.price_change) AS low_priceVariation,
    MAX(ocp.max_price) AS Highest_price,
    MIN(ocp.min_price) AS lowest_price
FROM
    dim_car dc join car c on c.manufacturer=dc.manufacturer
	inner join car_price pc on pc.carid = c.carid
	inner join new_car_prices ncp on ncp.carpriceid = pc.carpriceid
	inner join old_car_prices ocp on ocp.carpriceid = pc.carpriceid
    inner JOIN date_dim dd ON dd.date = ncp.date_range
	inner join features f on f.carid = c.carid
	 inner join year y on y.yearid = f.yearid
    inner JOIN Dim_gear_box dgb ON dgb.dim_gear_boxid = f.gearboxID
GROUP BY
    dc.dim_carID,
    dd.date_id,
    y.yearid,
    dgb.dim_gear_boxid


CREATE INDEX fact_car_dim_carID_idx ON fact_car(dim_carID);
CREATE INDEX fact_car_date_iD_idx ON fact_car(date_iD);
CREATE INDEX fact_car_dim_ProdYeariD_idx ON fact_car(dim_ProdYeariD);
CREATE INDEX fact_car_dim_gear_boxiD_idx ON fact_car(dim_gear_boxiD);
