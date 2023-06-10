
-- Creation of normalized table from staging tables


CREATE TABLE Year (
  yearid SERIAL  PRIMARY KEY,
  prod_year varchar(255),
created_time  timestamp,
  last_modified  timestamp ,
expired_time  timestamp 

);
INSERT INTO Year(prod_year)
SELECT DISTINCT prod_Year
FROM (
  SELECT ProdYear as prod_year FROM staging_car
  UNION
  SELECT Prod_Year as prod_year FROM staging_new_car
  UNION
  SELECT ProdYear as prod_year FROM staging_old_car
) AS y;


CREATE TABLE Fuel (
  Fuelid  SERIAL PRIMARY KEY,
  Fuel_type varchar(255),
created_time  timestamp,
  last_modified  timestamp ,
expired_time  timestamp 

);

INSERT INTO Fuel(Fuel_type)
SELECT DISTINCT fuel_type
FROM staging_car


CREATE TABLE Color (
  Colorid SERIAL PRIMARY KEY,
  Color varchar(255),
created_time  timestamp,
  last_modified  timestamp ,
expired_time  timestamp 

);
INSERT INTO Color(Color)
SELECT DISTINCT color
FROM staging_car

CREATE TABLE Interior (
  Interiorid SERIAL PRIMARY KEY,
  Leather_interior varchar(255),
created_time  timestamp,
  last_modified  timestamp ,
expired_time  timestamp 
);
INSERT INTO Interior(leather_interior)
SELECT DISTINCT leather_interior
FROM staging_car


CREATE TABLE Category (
  Categoryid  SERIAL PRIMARY KEY,
  category_name varchar(255),
created_time  timestamp,
  last_modified  timestamp ,
expired_time  timestamp 
);
INSERT INTO category(category_name)
SELECT DISTINCT category
FROM staging_car

CREATE TABLE Gear_box (
  GearBoxid SERIAL PRIMARY KEY,
  Gear_box_type varchar(255),
	created_time  timestamp,
  last_modified  timestamp ,
expired_time  timestamp 
);
INSERT INTO Gear_box(Gear_box_type)
SELECT DISTINCT Gear_box_type
FROM staging_car


CREATE TABLE Car (
  carid SERIAL PRIMARY KEY,
  Manufacturer varchar(255),
created_time  timestamp,
  last_modified  timestamp ,
expired_time  timestamp 

);
INSERT INTO car (manufacturer)
SELECT DISTINCT LOWER(manufacturer) AS manufacturer
FROM (
  SELECT manufacturer FROM staging_car
  UNION
  SELECT manufacturer FROM staging_new_car
  UNION
  SELECT manufacturer FROM staging_old_car
) AS d;

CREATE TABLE Model (
  ModelID SERIAL PRIMARY KEY,
  Model_name varchar(255),
created_time  timestamp,
  last_modified  timestamp ,
expired_time  timestamp 
);
INSERT INTO Model(Model_name)
SELECT DISTINCT Model
FROM 
(
	select model from staging_car
	Union
	select model from staging_new_car
	UNION
	select model from staging_old_car
) as m;

CREATE TABLE Car_Price
(
	carpriceId SERIAL PRIMARY KEY,
	ModelId int,
	FOREIGN KEY (ModelID) REFERENCES Model(ModelID),
	 carid int,
	FOREIGN KEY (CarID) REFERENCES Car(carID),
	 YearId int ,
	FOREIGN KEY (YearID) REFERENCES Year(YearID),
	Condition varchar(255)
);
INSERT INTO car_price(ModelID,CarID,YearID,condition)
SELECT distinct ModelID,CarID,YearID, condition
FROM 
(

	select  modelid,carid,yearId,1 as condition from staging_new_car snc
	join model m On snc.model=m.model_name
	join car c ON c.manufacturer=snc.manufacturer
	join year y ON y.prod_year=snc.prodyear
	UNION
	select  modelid,carid,yearId,0 as condition from staging_old_car soc
	join model m ON soc.model=m.model_name
	join car c ON c.manufacturer=soc.manufacturer
	join year y ON y.prod_year=soc.prodyear
) as m;


create table New_car_prices
(
	new_car_priceid serial primary key,
	carpriceid int,
	foreign key (carpriceid) references car_price(carpriceid),
	date_range date,
	new_price int,
	price_change int,
	price_variation varchar(255)
)
INSERT INTO new_car_prices(carpriceid,date_range,new_price, price_change,price_variation)
select distinct carpriceid, date_range,new_price, price_change,price_variation 
FROM(
	select carpriceid,date_range,new_price, price_change,price_variation from staging_new_car s
	join car_price cp on cp.modelid = (select modelid from model where model_name = s.model)
	and cp.carid = (select carid from car where manufacturer = s.manufacturer)
	and  cp.yearid = (select yearid from year where prod_year = s.prodyear)
)as np;

create table old_car_prices
(
	old_car_priceid serial primary key,
	carpriceid int,
	foreign key (carpriceid) references car_price(carpriceid),
	month_year date,
	max_price int,
	min_price int,
avg_price
)
INSERT INTO old_car_prices(carpriceid,month_year,max_price, min_price, avg_price)
select distinct carpriceid,month_year,max_price, min_price 
FROM(
	select carpriceid,month_year,maximum_price as max_price, minimum_price as min_price, avereage_price from staging_old_car s
	join car_price cp on cp.modelid = (select modelid from model where model_name = s.model)
	and cp.carid = (select carid from car where manufacturer = s.manufacturer)
	and  cp.yearid = (select yearid from year where prod_year = s.prodyear)
)as op;


CREATE TABLE features (
    FeatureID SERIAL PRIMARY KEY,
    ModelID INT,
	categoryID INT,
	InteriorID Int,
	ColorID INT,
	FuelID INT,
	GearBoxID INT,
	carid int,
	yearid int,
    FOREIGN KEY (ModelID) REFERENCES Model(ModelID),
	FOREIGN KEY (categoryID) REFERENCES category(CategoryID),
	FOREIGN KEY (InteriorID) REFERENCES Interior(InteriorID),
    FOREIGN KEY (ColorID) REFERENCES Color(ColorID),
	FOREIGN KEY (FuelID) REFERENCES Fuel(FuelID),
	FOREIGN KEY (GearboxId) REFERENCES Gear_box(gearBoxID),
	FOREIGN KEY (carid) REFERENCES car(carid),
	FOREIGN KEY (yearid) REFERENCES year(yearid),
	Mileage NUMERIC,
	CYLINDERS INT,
	AIRBAGS Int,
	engine_volume NUMERIC,
    created_time timestamp,
    last_modified timestamp,
    expired_time timestamp
);
INSERT INTO features(ModelID,CarID,YearID,categoryid,interiorid, colorid,fuelid,
					 gearboxid,mileage, cylinders, airbags, engine_volume)
SELECT distinct ModelID,CarID,YearID,categoryid,interiorid, colorid,fuelid,
					 gearboxid,mileage, cylinders, airbags, engine_volume
FROM 
(
	select  ModelID,CarID,YearID,categoryid,interiorid, colorid,fuelid,
		    gearboxid,mileage, cylinders, airbags, engine_volume 
	from staging_car s
	join model m On s.model=m.model_name
	join car c ON c.manufacturer=s.manufacturer
	join year y ON y.prod_year=s.prodyear
	join category ca on ca.category_name=s.category
	join interior i on i.leather_interior=s.leather_interior
	join color co on co.color=s.color
	join fuel f on f.fuel_type=s.fuel_type
	join gear_box g on g.gear_box_type=s.gear_box_type
) as f
