CREATE TABLE IF NOT EXISTS dds.api_orders (
    id serial PRIMARY key NOT NULL,
	order_id varchar(30) unique NOT NULL,
    order_ts timestamp NOT NULL
    );

INSERT INTO dds.api_orders (order_id, order_ts)
SELECT order_id, order_ts::timestamp
FROM stg.api_deliveries as source
WHERE NOT EXISTS (
    SELECT 1
    FROM dds.api_orders as target
    WHERE target.order_id = source.order_id
);