CREATE TABLE IF NOT EXISTS dm.dm_courier_ledger (
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

INSERT INTO dm.dm_courier_ledger (courier_id, 
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
   FROM dds.fct_api_deliveries AS dfad
   LEFT JOIN dds.api_orders AS dao ON dao.id = dfad.order_id
   LEFT JOIN dds.api_couriers AS dac ON dac.id = dfad.courier_id)
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