CREATE TABLE IF NOT EXISTS dds.fct_api_deliveries (
    id serial NOT NULL,
    order_id int4 unique NOT NULL,
    order_ts timestamp NOT NULL,
    delivery_id int4 unique NOT NULL,
    courier_id int4 NOT NULL,
    delivery_address varchar(100) NOT NULL,
    delivery_ts timestamp NOT NULL,
    rate int2 NOT NULL,
    sum numeric(10,2) NOT NULL,
    tip_sum numeric(10,2) NOT NULL
    CONSTRAINT fct_api_deliveries_rate_check CHECK ((rate::int2 >= (0))),
	CONSTRAINT fct_api_deliveries_sum_check CHECK ((sum::numeric(10,2) >= (0))),
	CONSTRAINT fct_api_deliveries_tip_sum_check CHECK ((tip_sum::numeric(10,2) >= 0)),
	CONSTRAINT fct_api_deliveries_pk PRIMARY KEY (id),
	CONSTRAINT fct_api_deliveries_rate_check_order_id_fkey FOREIGN KEY (order_id) REFERENCES dds.api_orders(id),
	CONSTRAINT fct_api_deliveries_rate_check_deliveries_id_fkey FOREIGN KEY (delivery_id) REFERENCES dds.api_deliveries(id),
    CONSTRAINT fct_api_deliveries_rate_check_courier_id_fkey FOREIGN KEY (courier_id) REFERENCES dds.api_couriers(id)
    );

INSERT INTO dds.fct_api_deliveries (order_id, 
                                    order_ts, 
                                    delivery_id, 
                                    courier_id, 
                                    delivery_address, 
                                    delivery_ts, 
                                    rate, 
                                    sum, 
                                    tip_sum)
SELECT dao.id AS order_id,
       dao.order_ts,
       dad.id AS delivery_id,
       dac.id AS courier_id,
       sad."address",
       dad.delivery_ts,
       sad.rate,
       sad.sum,
       sad.tip_sum
FROM stg.api_deliveries AS sad
LEFT JOIN dds.api_deliveries AS dad ON sad.delivery_id = dad.delivery_id
LEFT JOIN dds.api_orders AS dao ON sad.order_id = dao.order_id
LEFT JOIN dds.api_couriers AS dac ON sad.courier_id = dac.courier_id
AND sad.delivery_ts::TIMESTAMP >= dac.actual_ts_from
AND sad.delivery_ts::TIMESTAMP < dac.actual_ts_to
LEFT JOIN dds.fct_api_deliveries AS dfad ON dao.id = dfad.order_id
AND dad.id = dfad.delivery_id
WHERE dfad.order_id IS NULL;