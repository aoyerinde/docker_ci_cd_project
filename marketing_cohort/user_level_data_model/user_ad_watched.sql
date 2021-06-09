CREATE TABLE IF NOT EXISTS KG_DWH.L2_MARKETING_USER_LEVEL_MODEL.USER_AD_WATCHED
(
    INSTALL_TIME           TIMESTAMP_NTZ,
    APPSFLYER_DEVICE_ID    VARCHAR,
    AD_WATCHED_COUNT       NUMBER,
    D01_AD_WATCHED_COUNT   NUMBER,
    D02_AD_WATCHED_COUNT   NUMBER,
    D03_AD_WATCHED_COUNT   NUMBER,
    D04_AD_WATCHED_COUNT   NUMBER,
    D05_AD_WATCHED_COUNT   NUMBER,
    D06_AD_WATCHED_COUNT   NUMBER,
    D07_AD_WATCHED_COUNT   NUMBER,
    D14_AD_WATCHED_COUNT   NUMBER,
    D30_AD_WATCHED_COUNT   NUMBER,
    D60_AD_WATCHED_COUNT   NUMBER,
    D90_AD_WATCHED_COUNT   NUMBER,
    AD_WATCHED_REVENUE     FLOAT,
    D01_AD_WATCHED_REVENUE FLOAT,
    D02_AD_WATCHED_REVENUE FLOAT,
    D03_AD_WATCHED_REVENUE FLOAT,
    D04_AD_WATCHED_REVENUE FLOAT,
    D05_AD_WATCHED_REVENUE FLOAT,
    D06_AD_WATCHED_REVENUE FLOAT,
    D07_AD_WATCHED_REVENUE FLOAT,
    D14_AD_WATCHED_REVENUE FLOAT,
    D30_AD_WATCHED_REVENUE FLOAT,
    D60_AD_WATCHED_REVENUE FLOAT,
    D90_AD_WATCHED_REVENUE FLOAT,
    INSERT_TIME            TIMESTAMP_NTZ,
    _TST                   TIMESTAMP_NTZ

);

BEGIN;

TRUNCATE TABLE KG_DWH.L2_MARKETING_USER_LEVEL_MODEL.USER_AD_WATCHED;

INSERT INTO KG_DWH.L2_MARKETING_USER_LEVEL_MODEL.USER_AD_WATCHED

WITH AD_WATCHED AS
         (SELECT INSTALL_TIME,
                 APPSFLYER_DEVICE_ID,
                 DAYS_SINCE_INSTALL,
                 COUNTRY_CODE,
                 EVENT_TIME,
                 GAME_NAME,
                 PLATFORM
          FROM L1_APPSFLYER_PUSH_API_EVENT.APPSFLYER_AD_WATCHED),

     ---Ad_revenue
     AD_REV AS (
         SELECT DATE         AS EVENT_DATE,
                COUNTRY_CODE AS COUNTRY_CODE,
                GAME_NAME    AS GAME_NAME,
                PLATFORM     AS PLATFORM,
                REVENUE      AS REVENUE
         FROM L2_ADOPS_MODEL.AD_REVENUE),

     --Combine Ad_rev and ad_count
     AD_REV_COUNT AS
         (SELECT AD_REV.EVENT_DATE            EVENT_DATE,
                 AD_REV.COUNTRY_CODE       AS COUNTRY_CODE,
                 AD_REV.GAME_NAME          AS GAME_NAME,
                 AD_REV.PLATFORM           AS PLATFORM,
                 AD_REV.REVENUE            AS REVENUE,
                 AD_COUNT.AD_WATCHED_COUNT AS AD_WATCHED_COUNT
          FROM AD_REV
                   LEFT OUTER JOIN (SELECT *
                                    FROM L2_MARKETING_COHORT_MODEL.AD_WATCHED_COUNT) AD_COUNT
                                   ON AD_REV.EVENT_DATE = ad_count.EVENT_DATE
                                       AND AD_REV.COUNTRY_CODE = ad_count.COUNTRY_CODE
                                       AND AD_REV.GAME_NAME = ad_count.GAME_NAME
                                       AND AD_REV.PLATFORM = ad_count.PLATFORM),
     ---select for rev_per_ecap
     REV_PER_ECAP AS (
         SELECT AD_REV_COUNT.EVENT_DATE,
                AD_REV_COUNT.COUNTRY_CODE,
                AD_REV_COUNT.GAME_NAME,
                AD_REV_COUNT.PLATFORM,
                COALESCE(SUM(REVENUE) / SUM(AD_WATCHED_COUNT), 0) AS REV_PER_ECAP
         FROM AD_REV_COUNT

         GROUP BY EVENT_DATE,
                  COUNTRY_CODE,
                  GAME_NAME,
                  PLATFORM)


---Calculate your measures
SELECT INSTALL_TIME,
       APPSFLYER_DEVICE_ID,
       COUNT(*)                       AS                  AD_WATCHED_COUNT,
       COUNT(IFF(DAYS_SINCE_INSTALL < 1, 1, NULL))        D01_AD_WATCHED_COUNT,
       COUNT(IFF(DAYS_SINCE_INSTALL < 2, 1, NULL))        D02_AD_WATCHED_COUNT,
       COUNT(IFF(DAYS_SINCE_INSTALL < 3, 1, NULL))        D03_AD_WATCHED_COUNT,
       COUNT(IFF(DAYS_SINCE_INSTALL < 4, 1, NULL))        D04_AD_WATCHED_COUNT,
       COUNT(IFF(DAYS_SINCE_INSTALL < 5, 1, NULL))        D05_AD_WATCHED_COUNT,
       COUNT(IFF(DAYS_SINCE_INSTALL < 6, 1, NULL))        D06_AD_WATCHED_COUNT,
       COUNT(IFF(DAYS_SINCE_INSTALL < 7, 1, NULL))        D07_AD_WATCHED_COUNT,
       COUNT(IFF(DAYS_SINCE_INSTALL < 14, 1, NULL))       D14_AD_WATCHED_COUNT,
       COUNT(IFF(DAYS_SINCE_INSTALL < 30, 1, NULL))       D30_AD_WATCHED_COUNT,
       COUNT(IFF(DAYS_SINCE_INSTALL < 60, 1, NULL))       D60_AD_WATCHED_COUNT,
       COUNT(IFF(DAYS_SINCE_INSTALL < 90, 1, NULL))       D90_AD_WATCHED_COUNT,
       COALESCE(SUM(REV_PER_ECAP), 0) AS                  AD_WATCHED_REVENUE,
       SUM(IFF(DAYS_SINCE_INSTALL < 1, REV_PER_ECAP, 0))  D01_AD_WATCHED_REVENUE,
       SUM(IFF(DAYS_SINCE_INSTALL < 2, REV_PER_ECAP, 0))  D02_AD_WATCHED_REVENUE,
       SUM(IFF(DAYS_SINCE_INSTALL < 3, REV_PER_ECAP, 0))  D03_AD_WATCHED_REVENUE,
       SUM(IFF(DAYS_SINCE_INSTALL < 4, REV_PER_ECAP, 0))  D04_AD_WATCHED_REVENUE,
       SUM(IFF(DAYS_SINCE_INSTALL < 5, REV_PER_ECAP, 0))  D05_AD_WATCHED_REVENUE,
       SUM(IFF(DAYS_SINCE_INSTALL < 6, REV_PER_ECAP, 0))  D06_AD_WATCHED_REVENUE,
       SUM(IFF(DAYS_SINCE_INSTALL < 7, REV_PER_ECAP, 0))  D07_AD_WATCHED_REVENUE,
       SUM(IFF(DAYS_SINCE_INSTALL < 14, REV_PER_ECAP, 0)) D14_AD_WATCHED_REVENUE,
       SUM(IFF(DAYS_SINCE_INSTALL < 30, REV_PER_ECAP, 0)) D30_AD_WATCHED_REVENUE,
       SUM(IFF(DAYS_SINCE_INSTALL < 60, REV_PER_ECAP, 0)) D60_AD_WATCHED_REVENUE,
       SUM(IFF(DAYS_SINCE_INSTALL < 90, REV_PER_ECAP, 0)) D90_AD_WATCHED_REVENUE,
       CURRENT_TIMESTAMP::TIMESTAMP_NTZ                   INSERT_TIME,
       TO_DATE(INSTALL_TIME)::TIMESTAMP_NTZ               _TST

FROM (
         ---Bring Ad_watched and REV_ECAP  together
         SELECT INSTALL_TIME,
                APPSFLYER_DEVICE_ID,
                DAYS_SINCE_INSTALL,
                AD_WATCHED.COUNTRY_CODE COUNTRY_CODE,
                REV_PER_ECAP
         FROM AD_WATCHED
                  LEFT OUTER JOIN REV_PER_ECAP
                                  ON REV_PER_ECAP.EVENT_DATE = TO_DATE(AD_WATCHED.EVENT_TIME)
                                      AND REV_PER_ECAP.COUNTRY_CODE = AD_WATCHED.COUNTRY_CODE
                                      AND REV_PER_ECAP.GAME_NAME = AD_WATCHED.GAME_NAME
                                      AND REV_PER_ECAP.PLATFORM = AD_WATCHED.PLATFORM)


GROUP BY INSTALL_TIME, APPSFLYER_DEVICE_ID
;
COMMIT;