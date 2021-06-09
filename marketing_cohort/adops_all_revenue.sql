BEGIN;

CREATE OR REPLACE VIEW KG_DWH.L2_ADOPS_MODEL.AD_REVENUE AS

SELECT DATE,
       COUNTRY_CODE COUNTRY_CODE,
       GAME_NAME,
       PLATFORM     PLATFORM,
       SUM(REVENUE) REVENUE
FROM (
---APPLOVIN MAX REVENUES
         (
             SELECT DATE        AS DATE,
                    COUNTRYCODE AS COUNTRY_CODE,
                    CASE
                        WHEN BUNDLEIDENTIFIER = 'com.fluffyfairygames.idleminertycoon' THEN 'Idle Miner Tycoon'
                        WHEN BUNDLEIDENTIFIER = 'com.fluffyfairygames.idlefactorytycoon' THEN 'Idle Factory Tycoon'
                        END
                                AS GAME_NAME,
                    PLATFORM    AS PLATFORM,
                    REVENUE     AS REVENUE
             FROM KG_DWH.L1_ADOPS.APPLOVIN_MAX_ADOPS
             WHERE BUNDLEIDENTIFIER IN
                   ('com.fluffyfairygames.idleminertycoon', 'com.fluffyfairygames.idlefactorytycoon')
               AND LOWER(PLATFORM) IN ('ios', 'android')
         )
         UNION ALL
--- IRONSOURCE REVENUE
         (
             SELECT DATE        AS DATE,
                    COUNTRYCODE AS COUNTRY_CODE,
                    CASE
                        WHEN APP = 'com.fluffyfairygames.idleminertycoon' THEN 'Idle Miner Tycoon'
                        WHEN APP = 'com.fluffyfairygames.idlefactorytycoon' THEN 'Idle Factory Tycoon'
                        END
                                AS GAME_NAME,
                    PLATFORM    AS PLATFORM,
                    REVENUE     AS REVENUE
             FROM KG_DWH.L1_ADOPS.IRONSOURCE_ADOPS
             WHERE APP IN ('com.fluffyfairygames.idleminertycoon', 'com.fluffyfairygames.idlefactorytycoon')
               AND LOWER(PLATFORM) IN ('ios', 'android')
         )
     )
GROUP BY DATE, COUNTRY_CODE, GAME_NAME, PLATFORM
;


COMMIT;
