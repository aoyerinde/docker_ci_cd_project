CREATE TABLE IF NOT EXISTS POC.AIRFLOW_SNOWFLAKE_2 (
  date DATE,
  count NUMBER,
  number NUMBER,
  INSERTTIME TIMESTAMP);

BEGIN;

DELETE FROM POC.AIRFLOW_SNOWFLAKE_2
WHERE date >= '{{ ds }}'
  AND date < '{{ tomorrow_ds }}';

INSERT INTO POC.AIRFLOW_SNOWFLAKE_2
SELECT date, COUNT(*), MAX(number), current_timestamp::TIMESTAMP
FROM POC.AIRFLOW_IMPORT
WHERE date >= '{{ ds }}'
  AND date < '{{ tomorrow_ds }}'
GROUP BY date;

COMMIT;
