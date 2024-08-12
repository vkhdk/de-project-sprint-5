CREATE TABLE IF NOT EXISTS dds.api_couriers (
    id serial PRIMARY key NOT NULL,
    courier_id varchar(30) NOT NULL,
    courier_name varchar(100) NOT NULL,
    updated_ts timestamp NOT NULL DEFAULT now()
    );

INSERT INTO dds.api_couriers (courier_id, courier_name)
SELECT source._id, source.name
FROM stg.api_couriers as source
LEFT JOIN dds.api_couriers as target ON source._id = target.courier_id
WHERE target.courier_id IS NULL OR target.courier_name <> source.name;