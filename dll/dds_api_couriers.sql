CREATE TABLE IF NOT EXISTS dds.api_couriers (
    id serial PRIMARY key NOT NULL,
    courier_id varchar(30) NOT NULL,
    courier_name varchar(100) NOT NULL,
    actual_ts_from timestamp NOT NULL,
    actual_ts_to timestamp NOT NULL
    );

BEGIN;

INSERT INTO dds.api_couriers (courier_id, courier_name, actual_ts_from, actual_ts_to)
SELECT ts._id,
       ts.name,
       '1900-01-01' AS actual_ts_from,
       '2999-12-31' AS actual_ts_to
FROM stg.api_couriers AS ts
WHERE ts._id NOT IN
    (SELECT courier_id
     FROM dds.api_couriers);


INSERT INTO dds.api_couriers (courier_id, courier_name, actual_ts_from, actual_ts_to)
SELECT dac.courier_id,
       sac.name,
       now(),
       '2999-12-31'
FROM dds.api_couriers AS dac
LEFT JOIN stg.api_couriers AS sac ON dac.courier_id = sac._id
AND dac.courier_name <> sac.name
WHERE 1=1
  AND sac._id IS NOT NULL
  AND dac.actual_ts_to = '2999-12-31';

UPDATE dds.api_couriers AS d
SET actual_ts_to = NOW()
FROM stg.api_couriers AS ts
WHERE 1=1
  AND d.courier_id = ts._id
  AND d.courier_name <> ts.name
  AND d.actual_ts_to = '2999-12-31';

COMMIT;