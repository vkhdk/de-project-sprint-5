CREATE TABLE IF NOT EXISTS dds.api_deliveries (
    id serial PRIMARY key NOT NULL,
    delivery_id varchar(30) unique NOT NULL,
    delivery_ts timestamp NOT NULL,
    delivery_address varchar(100) NOT NULL,
    rate int2 NOT NULL,
    sum numeric(10,2) NOT NULL,
    tip_sum numeric(10,2) NOT NULL
    );

INSERT INTO dds.api_deliveries (delivery_id, delivery_ts, delivery_address, rate, sum, tip_sum)
SELECT delivery_id, 
       delivery_ts::timestamp, 
       "address", 
       rate, 
       sum, 
       tip_sum
FROM stg.api_deliveries as source
WHERE NOT EXISTS (
    SELECT 1
    FROM dds.api_deliveries as target
    WHERE target.delivery_id = source.delivery_id
);