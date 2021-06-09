BEGIN;

CREATE TABLE IF NOT EXISTS {TARGET_TABLE}
(
    DATE DATE,
  	PROVIDERNAME VARCHAR,
  	PLATFORM VARCHAR,
  	BUNDLEIDENTIFIER VARCHAR,
  	COUNTRYCODE VARCHAR,
  	INSTANCE VARCHAR,
  	ADSOURCECHECKS NUMBER,
  	ADSOURCERESPONSES NUMBER,
  	REVENUE DOUBLE,
  	IMPRESSIONS NUMBER,
    _TST  TIMESTAMPNTZ,
    INSERT_TIME TIMESTAMPNTZ
);

DELETE
FROM {TARGET_TABLE}
WHERE {SLICE}
;

INSERT INTO {TARGET_TABLE}
WITH RAW AS (
    SELECT DATA: date::DATE                DATE,
           CASE
               WHEN DATA: network::VARCHAR = 'APPLOVIN_NETWORK' THEN 'AppLovin'
               WHEN DATA: network::VARCHAR = 'ADCOLONY_NETWORK' THEN 'AdColony'
               WHEN DATA: network::VARCHAR = 'ADMOB_NETWORK' THEN 'AdMob'
               WHEN DATA: network::VARCHAR = 'FACEBOOK_MEDIATE' THEN 'Facebook'
               WHEN DATA: network::VARCHAR = 'IRONSOURCE_NETWORK' THEN 'ironSource'
               WHEN DATA: network::VARCHAR = 'MINTEGRAL_NETWORK' THEN 'Mintegral'
               WHEN DATA: network::VARCHAR = 'TAPJOY_NETWORK' THEN 'Tapjoy'
               WHEN DATA: network::VARCHAR = 'UNITY_NETWORK' THEN 'Unity'
               WHEN DATA: network::VARCHAR = 'VUNGLE_NETWORK' THEN 'Vungle'
               WHEN DATA: network::VARCHAR = 'TIKTOK_NETWORK' THEN 'TikTok'
               ELSE DATA: network::VARCHAR
               END                         PROVIDERNAME,
           CASE
               WHEN DATA:platform::VARCHAR = 'android' THEN 'Android'
               WHEN DATA:platform::VARCHAR = 'ios' THEN 'iOS'
               END                         PLATFORM,
           DATA:package_name::VARCHAR      BUNDLEIDENTIFIER,
           UPPER(DATA:country::VARCHAR)    COUNTRYCODE,
           DATA:network_placement::VARCHAR INSTANCE,
           DATA:attempts::INT              ADSOURCECHECKS,
           DATA:responses::INT             ADSOURCERESPONSES,
           DATA:estimated_revenue::FLOAT   REVENUE,
           DATA:impressions::INT           IMPRESSIONS,
           _TST,
           CURRENT_TIMESTAMP               INSERT_TIME
    FROM {SOURCE_TABLE}
)
SELECT DATE,
       PROVIDERNAME,
       PLATFORM,
       BUNDLEIDENTIFIER,
       COUNTRYCODE,
       INSTANCE,
       SUM(ADSOURCECHECKS)    ADSOURCECHECKS,
       SUM(ADSOURCERESPONSES) ADSOURCERESPONSES,
       SUM(REVENUE)           REVENUE,
       SUM(IMPRESSIONS)       IMPRESSIONS,
       _TST,
       INSERT_TIME
FROM RAW
WHERE {SLICE}
GROUP BY DATE,
    PROVIDERNAME,
    PLATFORM,
    BUNDLEIDENTIFIER,
    COUNTRYCODE,
    INSTANCE,
    _TST,
    INSERT_TIME;

COMMIT;
