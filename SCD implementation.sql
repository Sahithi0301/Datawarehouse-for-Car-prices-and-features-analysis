-- Create trigger for scd type 2
CREATE OR REPLACE FUNCTION handle_dim_car_changes()
RETURNS TRIGGER AS $$
BEGIN
    -- INSERT operation
    IF (TG_OP = 'INSERT') THEN
        NEW.current_flag := TRUE;
        NEW.effective_timestamp := NOW();
        NEW.expired_timestamp := NULL;

    -- UPDATE operation
    ELSIF (TG_OP = 'UPDATE') THEN
        INSERT INTO dim_car (
            manufacturer,
            model,
            engine_volume,
            airbags,
            interior,
            color,
            fuel_type,
            current_flag,
            effective_timestamp,
            expired_timestamp
        )
        VALUES (
            NEW.manufacturer,
            NEW.model,
            NEW.engine_volume,
            NEW.airbags,
            NEW.interior,
            NEW.color,
            NEW.fuel_type,
            TRUE,
            NOW(),
            NULL
        );

        OLD.current_flag := FALSE;
		OLD. expired_timestamp := NOW();

        RETURN OLD;

    -- DELETE operation
    ELSIF (TG_OP = 'DELETE') THEN
        UPDATE dim_car
        SET
            current_flag = FALSE,
            expired_timestamp = NOW()
        WHERE
            dim_carid = OLD.dim_carid;

        RETURN NULL;
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER handle_changes_on_dim_car
BEFORE INSERT OR UPDATE OR DELETE
ON dim_car
FOR EACH ROW
EXECUTE FUNCTION handle_dim_car_changes();
DROP TRIGGER IF EXISTS handle_changes_on_dim_car ON dim_car;



--insert statement
INSERT INTO dim_car (manufacturer, model, engine_volume, airbags, interior, color, fuel_type)
VALUES('TOYOTA', 'INNOVA', 18, 4, 'YES', 'SILVER', 'PETROL');

-- UPDATE example
UPDATE dim_car
SET  engine_volume = 20, color='Black'
WHERE dim_carid = 5424 ;

-- DELETE example
delete from dim_car where dim_carid=1035
select * from dim_car where dim_carid=1035




CREATE OR REPLACE PROCEDURE update_dim_car()
LANGUAGE plpgsql
AS $$
BEGIN
    -- INSERT event
    INSERT INTO dim_car (featureid, manufacturer, model, engine_volume, airbags, interior, color, fuel_type, current_flag, effective_timestamp, expired_timestamp)
    SELECT c.featureid, c.manufacturer, m.model_name, c.engine_volume, c.airbags, i.leather_interior, co.color, fu.fuel_type, TRUE, NOW(), NULL
    FROM features c
    JOIN model m ON m.model_name = c.model
    JOIN interior i ON i.leather_interior = c.leather_interior
    JOIN color co ON co.color = c.color
    JOIN fuel fu ON fu.fuel_type = c.fuel_type
    EXCEPT
    SELECT d.featureid, d.manufacturer, d.model, d.engine_volume, d.airbags, d.interior, d.color, d.fuel_type, d.current_flag, d.effective_timestamp, d.expired_timestamp
    FROM dim_car d
    WHERE d.current_flag = TRUE;

	-- UPDATE event
UPDATE dim_car SET 
    current_flag = true
WHERE 
    featureid IN (
        SELECT 
            c.carid 
        FROM 
            features f
            JOIN car c ON f.carid = c.carid
            JOIN model m ON m.modelid = c.modelid
        WHERE 
            EXISTS (
                SELECT 1
                FROM dim_car d
                WHERE 
                    d.featureid = c.carid
                    AND d.manufacturer = c.manufacturer
                    AND d.model = m.model_name
                    AND (
                        d.engine_volume <> f.engine_volume
                        OR d.airbags <> f.airbags
                        OR d.interior <> f.leather_interior
                        OR d.color <> f.color
                        OR d.fuel_type <> f.fuel_type
                    )
                    AND d.current_flag = true
            )
    );

INSERT INTO dim_car (featureid, manufacturer, model, engine_volume, airbags, interior, color, fuel_type, current_flag)
SELECT 
    c.carid,
    c.manufacturer,
    m.model_name,
    f.engine_volume,
    f.airbags,
    f.leather_interior,
    f.color,
    f.fuel_type,
    true
FROM 
    features f
    JOIN car c ON f.carid = c.carid
    JOIN model m ON m.modelid = c.modelid
WHERE 
    (c.carid, c.manufacturer, m.model_name, f.engine_volume, f.airbags, f.leather_interior, f.color, f.fuel_type, 'Y')
    EXCEPT
    SELECT 
        d.featureid, d.manufacturer, d.model, d.engine_volume, d.airbags, d.interior, d.color, d.fuel_type, d.current_flag
    FROM 
        dim_car d
    WHERE 
        d.current_flag = true;
		
    -- DELETE event
    UPDATE dim_car SET 
        current_flag = false,
        expired_timestamp = NOW()
    WHERE 
        featureid IN (
            SELECT 
                c.carid 
            FROM 
                features f
                JOIN car c ON f.carid = c.carid
                JOIN model m ON m.modelid = c.modelid
            WHERE 
                EXISTS (
                    SELECT 1
                    FROM dim_car d
                    WHERE 
                        d.featureid = c.carid
                        AND d.manufacturer = c.manufacturer
                        AND d.model = m.model_name
                        AND d.current_flag = true
                )
        );

    DELETE FROM dim_car d
    USING features f
    WHERE d.featureid = f.featureid
        AND d.current_flag = false;
END;
$$;




INSERT INTO features(ModelID,CarID,YearID,categoryid,interiorid, colorid,fuelid,
					 gearboxid,mileage, cylinders, airbags, engine_volume)
values(211, 27, 3,7, 2, 16, 5, 1, 13000, 4,12, 3)
call update_dim_car()

update features 
set engine_volume=20
where featureId=4094
call update_dim_car()

delete from feature where featureid=2
call update_dim_car()
select * from dim_car where dim_carid=2
