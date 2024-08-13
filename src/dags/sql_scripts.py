dds_api_couriers = '''
CREATE TABLE IF NOT EXISTS {dds_schema_name}.api_couriers (
    id serial PRIMARY key NOT NULL,
    courier_id varchar(30) NOT NULL,
    courier_name varchar(100) NOT NULL,
    actual_ts_from timestamp NOT NULL,
    actual_ts_to timestamp NOT NULL
    );

BEGIN;

INSERT INTO {dds_schema_name}.api_couriers (courier_id, courier_name, actual_ts_from, actual_ts_to)
SELECT ts._id,
       ts.name,
       '1900-01-01' AS actual_ts_from,
       '2999-12-31' AS actual_ts_to
FROM {stage_schema_name}.api_couriers AS ts
WHERE ts._id NOT IN
    (SELECT courier_id
     FROM {dds_schema_name}.api_couriers);


INSERT INTO {dds_schema_name}.api_couriers (courier_id, courier_name, actual_ts_from, actual_ts_to)
SELECT dac.courier_id,
       sac.name,
       now(),
       '2999-12-31'
FROM {dds_schema_name}.api_couriers AS dac
LEFT JOIN {stage_schema_name}.api_couriers AS sac ON dac.courier_id = sac._id
AND dac.courier_name <> sac.name
WHERE 1=1
  AND sac._id IS NOT NULL
  AND dac.actual_ts_to = '2999-12-31';

UPDATE {dds_schema_name}.api_couriers AS d
SET actual_ts_to = NOW()
FROM {stage_schema_name}.api_couriers AS ts
WHERE 1=1
  AND d.courier_id = ts._id
  AND d.courier_name <> ts.name
  AND d.actual_ts_to = '2999-12-31';

COMMIT;
'''

dds_api_deliveries = '''
CREATE TABLE IF NOT EXISTS {dds_schema_name}.api_deliveries (
    id serial PRIMARY key NOT NULL,
    delivery_id varchar(30) unique NOT NULL,
    delivery_ts timestamp NOT NULL,
    delivery_address varchar(100) NOT NULL,
    rate int2 NOT NULL,
    sum numeric(10,2) NOT NULL,
    tip_sum numeric(10,2) NOT NULL
    );

INSERT INTO {dds_schema_name}.api_deliveries (delivery_id, delivery_ts, delivery_address, rate, sum, tip_sum)
SELECT delivery_id, 
       delivery_ts::timestamp, 
       "address", 
       rate, 
       sum, 
       tip_sum
FROM {stage_schema_name}.api_deliveries as source
WHERE NOT EXISTS (
    SELECT 1
    FROM {dds_schema_name}.api_deliveries as target
    WHERE target.delivery_id = source.delivery_id
);
'''

dds_api_orders = '''
CREATE TABLE IF NOT EXISTS {dds_schema_name}.api_orders (
    id serial PRIMARY key NOT NULL,
	order_id varchar(30) unique NOT NULL,
    order_ts timestamp NOT NULL
    );

INSERT INTO {dds_schema_name}.api_orders (order_id, order_ts)
SELECT order_id, order_ts::timestamp
FROM {stage_schema_name}.api_deliveries as source
WHERE NOT EXISTS (
    SELECT 1
    FROM {dds_schema_name}.api_orders as target
    WHERE target.order_id = source.order_id
);
'''

dds_fct_api_deliveries = '''
CREATE TABLE IF NOT EXISTS {dds_schema_name}.fct_api_deliveries (
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
	CONSTRAINT fct_api_deliveries_rate_check_order_id_fkey FOREIGN KEY (order_id) REFERENCES {dds_schema_name}.api_orders(id),
	CONSTRAINT fct_api_deliveries_rate_check_deliveries_id_fkey FOREIGN KEY (delivery_id) REFERENCES {dds_schema_name}.api_deliveries(id),
    CONSTRAINT fct_api_deliveries_rate_check_courier_id_fkey FOREIGN KEY (courier_id) REFERENCES {dds_schema_name}.api_couriers(id)
    );

INSERT INTO {dds_schema_name}.fct_api_deliveries (order_id, 
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
FROM {stage_schema_name}.api_deliveries AS sad
LEFT JOIN {dds_schema_name}.api_deliveries AS dad ON sad.delivery_id = dad.delivery_id
LEFT JOIN {dds_schema_name}.api_orders AS dao ON sad.order_id = dao.order_id
LEFT JOIN {dds_schema_name}.api_couriers AS dac ON sad.courier_id = dac.courier_id
AND sad.delivery_ts::TIMESTAMP >= dac.actual_ts_from
AND sad.delivery_ts::TIMESTAMP < dac.actual_ts_to
LEFT JOIN {dds_schema_name}.fct_api_deliveries AS dfad ON dao.id = dfad.order_id
AND dad.id = dfad.delivery_id
WHERE dfad.order_id IS NULL;
'''

dm_courier_ledger = '''
CREATE TABLE IF NOT EXISTS {stage_schema_name}.dm_courier_ledger (
    id SERIAL NOT NULL PRIMARY KEY,
    courier_id VARCHAR(30) NOT NULL,
    courier_name VARCHAR(30) NOT NULL,
    settlement_year INT NOT NULL CHECK((settlement_year>=2022) AND settlement_year <=2500),
    settlement_month INT NOT NULL CHECK((settlement_month>=1) AND settlement_month <=12),
    orders_count INT NOT NULL CHECK(orders_count>=0) DEFAULT 0,
    orders_total_sum numeric(14, 2) NOT NULL DEFAULT 0 CHECK (orders_total_sum >= (0)::numeric),
    rate_avg numeric(2, 1) NOT NULL DEFAULT 0 CHECK ((rate_avg >= (0)::numeric) AND (rate_avg <= (5)::numeric)),
    order_processing_fee numeric(14, 2) NOT NULL DEFAULT 0 CHECK (order_processing_fee >= (0)::numeric),
    courier_order_sum numeric(14, 2) NOT NULL DEFAULT 0 CHECK (courier_order_sum >= (0)::numeric),
    courier_tips_sum numeric(14, 2) NOT NULL DEFAULT 0 CHECK (courier_tips_sum >= (0)::numeric),
    courier_reward_sum numeric(14, 2) NOT NULL DEFAULT 0 CHECK (courier_reward_sum >= (0)::numeric),
    CONSTRAINT dm_settlement_courier_id_check UNIQUE (courier_id, courier_name, settlement_month, settlement_year)
    );

INSERT INTO {stage_schema_name}.dm_courier_ledger (courier_id, 
								  courier_name, 
								  settlement_year, 
								  settlement_month, 
								  orders_count, 
								  orders_total_sum, 
								  rate_avg, 
								  order_processing_fee, 
								  courier_order_sum, 
								  courier_tips_sum, 
								  courier_reward_sum) 
WITH sub_t AS
  (SELECT dac.courier_id,
          dac.courier_name,
          EXTRACT(YEAR
                  FROM dao.order_ts) AS YEAR,
          EXTRACT(MONTH
                  FROM dao.order_ts) AS MONTH,
          dao.order_id,
          dfad.sum,
          dfad.rate,
          dfad.tip_sum
   FROM {dds_schema_name}.fct_api_deliveries AS dfad
   LEFT JOIN {dds_schema_name}.api_orders AS dao ON dao.id = dfad.order_id
   LEFT JOIN {dds_schema_name}.api_couriers AS dac ON dac.id = dfad.courier_id)
SELECT courier_id,
       courier_name,
       YEAR AS settlement_year,
               MONTH AS settlement_month,
                        COUNT(order_id) AS orders_count,
                        SUM(SUM) AS orders_total_sum,
                        AVG(rate)::NUMERIC(14, 2) AS rate_avg,
                        SUM(SUM)*0.25 AS order_processing_fee,
                        CASE
                            WHEN AVG(rate)::NUMERIC(14, 2) < 4 THEN SUM(GREATEST(100, 0.05*SUM))
                            WHEN AVG(rate)::NUMERIC(14, 2) >= 4
                                 AND AVG(rate)::NUMERIC(14, 2) < 4.5 THEN SUM(GREATEST(150, 0.07*SUM))
                            WHEN AVG(rate)::NUMERIC(14, 2) >= 4.5
                                 AND AVG(rate)::NUMERIC(14, 2) < 4.9 THEN SUM(GREATEST(175, 0.08*SUM))
                            ELSE SUM(GREATEST(200, 0.1*SUM))
                        END AS courier_order_sum,
                        SUM(tip_sum) AS courier_tips_sum,
                        CASE
                            WHEN AVG(rate)::NUMERIC(14, 2) < 4 THEN SUM(GREATEST(100, 0.05*SUM)) + SUM(tip_sum)*0.95
                            WHEN AVG(rate)::NUMERIC(14, 2) >= 4
                                 AND AVG(rate)::NUMERIC(14, 2) < 4.5 THEN SUM(GREATEST(150, 0.07*SUM)) + SUM(tip_sum)*0.95
                            WHEN AVG(rate)::NUMERIC(14, 2) >= 4.5
                                 AND AVG(rate)::NUMERIC(14, 2) < 4.9 THEN SUM(GREATEST(175, 0.08*SUM)) + SUM(tip_sum)*0.95
                            ELSE SUM(GREATEST(200, 0.1*SUM)) + SUM(tip_sum)*0.95
                        END AS courier_reward_sum
FROM sub_t
GROUP BY courier_id,
         courier_name,
         YEAR,
         MONTH
ORDER BY courier_id,
         courier_name,
         YEAR,
         MONTH ASC ON CONFLICT (courier_id,
                                settlement_year,
                                courier_name,
                                settlement_month) DO
UPDATE
SET orders_count= EXCLUDED.orders_count,
    orders_total_sum= EXCLUDED.orders_total_sum,
    rate_avg= EXCLUDED.rate_avg,
    order_processing_fee= EXCLUDED.order_processing_fee,
    courier_order_sum= EXCLUDED.courier_order_sum,
    courier_tips_sum= EXCLUDED.courier_tips_sum,
    courier_reward_sum= EXCLUDED.courier_reward_sum;
'''