CREATE TABLE IF NOT EXISTS "KG_DWH"."L2_MARKETING_USER_LEVEL_MODEL"."USER_IN_APP_PURCHASE"
(

    INSTALL_TIME                               TIMESTAMP_NTZ,
    APPSFLYER_DEVICE_ID                        VARCHAR,
    PURCHASE_COUNT                             BIGINT,
    D01_PURCHASE_COUNT                         BIGINT,
    D02_PURCHASE_COUNT                         BIGINT,
    D03_PURCHASE_COUNT                         BIGINT,
    D04_PURCHASE_COUNT                         BIGINT,
    D05_PURCHASE_COUNT                         BIGINT,
    D06_PURCHASE_COUNT                         BIGINT,
    D07_PURCHASE_COUNT                         BIGINT,
    D14_PURCHASE_COUNT                         BIGINT,
    D30_PURCHASE_COUNT                         BIGINT,
    D60_PURCHASE_COUNT                         BIGINT,
    D90_PURCHASE_COUNT                         BIGINT,
    PURCHASE_REVENUE_GROSS                     FLOAT,
    D01_PURCHASE_REVENUE_GROSS                 FLOAT,
    D02_PURCHASE_REVENUE_GROSS                 FLOAT,
    D03_PURCHASE_REVENUE_GROSS                 FLOAT,
    D04_PURCHASE_REVENUE_GROSS                 FLOAT,
    D05_PURCHASE_REVENUE_GROSS                 FLOAT,
    D06_PURCHASE_REVENUE_GROSS                 FLOAT,
    D07_PURCHASE_REVENUE_GROSS                 FLOAT,
    D14_PURCHASE_REVENUE_GROSS                 FLOAT,
    D30_PURCHASE_REVENUE_GROSS                 FLOAT,
    D60_PURCHASE_REVENUE_GROSS                 FLOAT,
    D90_PURCHASE_REVENUE_GROSS                 FLOAT,
    PURCHASE_REVENUE_AFTER_SHARE               FLOAT,
    D01_PURCHASE_REVENUE_AFTER_SHARE           FLOAT,
    D02_PURCHASE_REVENUE_AFTER_SHARE           FLOAT,
    D03_PURCHASE_REVENUE_AFTER_SHARE           FLOAT,
    D04_PURCHASE_REVENUE_AFTER_SHARE           FLOAT,
    D05_PURCHASE_REVENUE_AFTER_SHARE           FLOAT,
    D06_PURCHASE_REVENUE_AFTER_SHARE           FLOAT,
    D07_PURCHASE_REVENUE_AFTER_SHARE           FLOAT,
    D14_PURCHASE_REVENUE_AFTER_SHARE           FLOAT,
    D30_PURCHASE_REVENUE_AFTER_SHARE           FLOAT,
    D60_PURCHASE_REVENUE_AFTER_SHARE           FLOAT,
    D90_PURCHASE_REVENUE_AFTER_SHARE           FLOAT,
    PURCHASE_REVENUE_AFTER_SHARE_AFTER_VAT     FLOAT,
    D01_PURCHASE_REVENUE_AFTER_SHARE_AFTER_VAT FLOAT,
    D02_PURCHASE_REVENUE_AFTER_SHARE_AFTER_VAT FLOAT,
    D03_PURCHASE_REVENUE_AFTER_SHARE_AFTER_VAT FLOAT,
    D04_PURCHASE_REVENUE_AFTER_SHARE_AFTER_VAT FLOAT,
    D05_PURCHASE_REVENUE_AFTER_SHARE_AFTER_VAT FLOAT,
    D06_PURCHASE_REVENUE_AFTER_SHARE_AFTER_VAT FLOAT,
    D07_PURCHASE_REVENUE_AFTER_SHARE_AFTER_VAT FLOAT,
    D14_PURCHASE_REVENUE_AFTER_SHARE_AFTER_VAT FLOAT,
    D30_PURCHASE_REVENUE_AFTER_SHARE_AFTER_VAT FLOAT,
    D60_PURCHASE_REVENUE_AFTER_SHARE_AFTER_VAT FLOAT,
    D90_PURCHASE_REVENUE_AFTER_SHARE_AFTER_VAT FLOAT,
    INSERT_TIME                                TIMESTAMP_NTZ,
    _TST                                       TIMESTAMP_NTZ
);

BEGIN;

TRUNCATE TABLE "KG_DWH"."L2_MARKETING_USER_LEVEL_MODEL"."USER_IN_APP_PURCHASE";


INSERT INTO L2_MARKETING_USER_LEVEL_MODEL.USER_IN_APP_PURCHASE

WITH PURCHASE_BY_EVENT_DATE AS (SELECT TO_DATE(EVENT_TIME)                                                              EVENT_DATE,
                                       APPSFLYER_DEVICE_ID,
                                       INSTALL_TIME,
                                       COUNTRY_CODE,
                                       COUNT(*)                                                                         PURCHASE_COUNT,
                                       SUM(PURCHASE_REVENUE_AFTER_SHARE)                                                TOTAL_PURCHASE_REVENUE_AFTER_SHARE,
                                       SUM(PURCHASE_REVENUE_AFTER_SHARE_AFTER_VAT)                                      TOTAL_PURCHASE_REVENUE_AFTER_SHARE_AFTER_VAT,
                                       SUM(PURCHASE_REVENUE_GROSS)                                                      TOTAL_PURCHASE_REVENUE_GROSS,


                                       COUNT(
                                               IFF(DAYS_SINCE_INSTALL < 1, REVENUE_IN_SELECTED_CURRENCY, NULL))         D01_PURCHASE_COUNT,
                                       COUNT(
                                               IFF(DAYS_SINCE_INSTALL < 2, REVENUE_IN_SELECTED_CURRENCY, NULL))         D02_PURCHASE_COUNT,

                                       COUNT(
                                               IFF(DAYS_SINCE_INSTALL < 3, REVENUE_IN_SELECTED_CURRENCY, NULL))         D03_PURCHASE_COUNT,
                                       COUNT(
                                               IFF(DAYS_SINCE_INSTALL < 4, REVENUE_IN_SELECTED_CURRENCY, NULL))         D04_PURCHASE_COUNT,
                                       COUNT(
                                               IFF(DAYS_SINCE_INSTALL < 5, REVENUE_IN_SELECTED_CURRENCY, NULL))         D05_PURCHASE_COUNT,
                                       COUNT(
                                               IFF(DAYS_SINCE_INSTALL < 6, REVENUE_IN_SELECTED_CURRENCY, NULL))         D06_PURCHASE_COUNT,
                                       COUNT(
                                               IFF(DAYS_SINCE_INSTALL < 7, REVENUE_IN_SELECTED_CURRENCY, NULL))         D07_PURCHASE_COUNT,
                                       COUNT(
                                               IFF(DAYS_SINCE_INSTALL < 14, REVENUE_IN_SELECTED_CURRENCY, NULL))        D14_PURCHASE_COUNT,
                                       COUNT(
                                               IFF(DAYS_SINCE_INSTALL < 30, REVENUE_IN_SELECTED_CURRENCY, NULL))        D30_PURCHASE_COUNT,
                                       COUNT(
                                               IFF(DAYS_SINCE_INSTALL < 60, REVENUE_IN_SELECTED_CURRENCY, NULL))        D60_PURCHASE_COUNT,
                                       COUNT(
                                               IFF(DAYS_SINCE_INSTALL < 90, REVENUE_IN_SELECTED_CURRENCY, NULL))        D90_PURCHASE_COUNT,

                                       SUM(
                                               IFF(DAYS_SINCE_INSTALL < 1, PURCHASE_REVENUE_GROSS, 0))                  D01_PURCHASE_REVENUE_GROSS,
                                       SUM(
                                               IFF(DAYS_SINCE_INSTALL < 2, PURCHASE_REVENUE_GROSS, 0))                  D02_PURCHASE_REVENUE_GROSS,
                                       SUM(
                                               IFF(DAYS_SINCE_INSTALL < 3, PURCHASE_REVENUE_GROSS, 0))                  D03_PURCHASE_REVENUE_GROSS,
                                       SUM(
                                               IFF(DAYS_SINCE_INSTALL < 4, PURCHASE_REVENUE_GROSS, 0))                  D04_PURCHASE_REVENUE_GROSS,
                                       SUM(
                                               IFF(DAYS_SINCE_INSTALL < 5, PURCHASE_REVENUE_GROSS, 0))                  D05_PURCHASE_REVENUE_GROSS,
                                       SUM(
                                               IFF(DAYS_SINCE_INSTALL < 6, PURCHASE_REVENUE_GROSS, 0))                  D06_PURCHASE_REVENUE_GROSS,
                                       SUM(
                                               IFF(DAYS_SINCE_INSTALL < 7, PURCHASE_REVENUE_GROSS, 0))                  D07_PURCHASE_REVENUE_GROSS,
                                       SUM(
                                               IFF(DAYS_SINCE_INSTALL < 14, PURCHASE_REVENUE_GROSS, 0))                 D14_PURCHASE_REVENUE_GROSS,
                                       SUM(
                                               IFF(DAYS_SINCE_INSTALL < 30, PURCHASE_REVENUE_GROSS, 0))                 D30_PURCHASE_REVENUE_GROSS,
                                       SUM(
                                               IFF(DAYS_SINCE_INSTALL < 60, PURCHASE_REVENUE_GROSS, 0))                 D60_PURCHASE_REVENUE_GROSS,
                                       SUM(
                                               IFF(DAYS_SINCE_INSTALL < 90, PURCHASE_REVENUE_GROSS, 0))                 D90_PURCHASE_REVENUE_GROSS,


                                       SUM(IFF(DAYS_SINCE_INSTALL < 1, PURCHASE_REVENUE_AFTER_SHARE, 0))                D01_PURCHASE_REVENUE_AFTER_SHARE,
                                       SUM(IFF(DAYS_SINCE_INSTALL < 2, PURCHASE_REVENUE_AFTER_SHARE, 0))                D02_PURCHASE_REVENUE_AFTER_SHARE,
                                       SUM(IFF(DAYS_SINCE_INSTALL < 3, PURCHASE_REVENUE_AFTER_SHARE, 0))                D03_PURCHASE_REVENUE_AFTER_SHARE,
                                       SUM(IFF(DAYS_SINCE_INSTALL < 4, PURCHASE_REVENUE_AFTER_SHARE, 0))                D04_PURCHASE_REVENUE_AFTER_SHARE,
                                       SUM(IFF(DAYS_SINCE_INSTALL < 5, PURCHASE_REVENUE_AFTER_SHARE, 0))                D05_PURCHASE_REVENUE_AFTER_SHARE,
                                       SUM(IFF(DAYS_SINCE_INSTALL < 6, PURCHASE_REVENUE_AFTER_SHARE, 0))                D06_PURCHASE_REVENUE_AFTER_SHARE,
                                       SUM(IFF(DAYS_SINCE_INSTALL < 7, PURCHASE_REVENUE_AFTER_SHARE, 0))                D07_PURCHASE_REVENUE_AFTER_SHARE,
                                       SUM(IFF(DAYS_SINCE_INSTALL < 14, PURCHASE_REVENUE_AFTER_SHARE, 0))               D14_PURCHASE_REVENUE_AFTER_SHARE,
                                       SUM(IFF(DAYS_SINCE_INSTALL < 30, PURCHASE_REVENUE_AFTER_SHARE, 0))               D30_PURCHASE_REVENUE_AFTER_SHARE,
                                       SUM(IFF(DAYS_SINCE_INSTALL < 60, PURCHASE_REVENUE_AFTER_SHARE, 0))               D60_PURCHASE_REVENUE_AFTER_SHARE,
                                       SUM(IFF(DAYS_SINCE_INSTALL < 90, PURCHASE_REVENUE_AFTER_SHARE, 0))               D90_PURCHASE_REVENUE_AFTER_SHARE,

                                       SUM(
                                               IFF(DAYS_SINCE_INSTALL < 1, PURCHASE_REVENUE_AFTER_SHARE_AFTER_VAT, 0))  D01_PURCHASE_REVENUE_AFTER_SHARE_AFTER_VAT,
                                       SUM(
                                               IFF(DAYS_SINCE_INSTALL < 2, PURCHASE_REVENUE_AFTER_SHARE_AFTER_VAT, 0))  D02_PURCHASE_REVENUE_AFTER_SHARE_AFTER_VAT,
                                       SUM(
                                               IFF(DAYS_SINCE_INSTALL < 3, PURCHASE_REVENUE_AFTER_SHARE_AFTER_VAT, 0))  D03_PURCHASE_REVENUE_AFTER_SHARE_AFTER_VAT,
                                       SUM(
                                               IFF(DAYS_SINCE_INSTALL < 4, PURCHASE_REVENUE_AFTER_SHARE_AFTER_VAT, 0))  D04_PURCHASE_REVENUE_AFTER_SHARE_AFTER_VAT,
                                       SUM(
                                               IFF(DAYS_SINCE_INSTALL < 5, PURCHASE_REVENUE_AFTER_SHARE_AFTER_VAT, 0))  D05_PURCHASE_REVENUE_AFTER_SHARE_AFTER_VAT,
                                       SUM(
                                               IFF(DAYS_SINCE_INSTALL < 6, PURCHASE_REVENUE_AFTER_SHARE_AFTER_VAT, 0))  D06_PURCHASE_REVENUE_AFTER_SHARE_AFTER_VAT,

                                       SUM(
                                               IFF(DAYS_SINCE_INSTALL < 7, PURCHASE_REVENUE_AFTER_SHARE_AFTER_VAT, 0))  D07_PURCHASE_REVENUE_AFTER_SHARE_AFTER_VAT,
                                       SUM(
                                               IFF(DAYS_SINCE_INSTALL < 14, PURCHASE_REVENUE_AFTER_SHARE_AFTER_VAT, 0)) D14_PURCHASE_REVENUE_AFTER_SHARE_AFTER_VAT,
                                       SUM(
                                               IFF(DAYS_SINCE_INSTALL < 30, PURCHASE_REVENUE_AFTER_SHARE_AFTER_VAT, 0)) D30_PURCHASE_REVENUE_AFTER_SHARE_AFTER_VAT,
                                       SUM(
                                               IFF(DAYS_SINCE_INSTALL < 60, PURCHASE_REVENUE_AFTER_SHARE_AFTER_VAT, 0)) D60_PURCHASE_REVENUE_AFTER_SHARE_AFTER_VAT,
                                       SUM(
                                               IFF(DAYS_SINCE_INSTALL < 90, PURCHASE_REVENUE_AFTER_SHARE_AFTER_VAT, 0)) D90_PURCHASE_REVENUE_AFTER_SHARE_AFTER_VAT

                                FROM "KG_DWH"."L2_APPSFLYER_PUSH_API_EVENT"."APPSFLYER_IN_APP_PURCHASE_WITH_EXCHANGE_RATE_AND_VAT"
                                GROUP BY INSTALL_TIME, APPSFLYER_DEVICE_ID, EVENT_DATE, COUNTRY_CODE
)


SELECT INSTALL_TIME,
       APPSFLYER_DEVICE_ID,

       SUM(PURCHASE_COUNT)                                                          PURCHASE_COUNT,
       SUM(D01_PURCHASE_COUNT)                                                      D01_PURCHASE_COUNT,
       SUM(D02_PURCHASE_COUNT)                                                      D02_PURCHASE_COUNT,
       SUM(D03_PURCHASE_COUNT)                                                      D03_PURCHASE_COUNT,
       SUM(D04_PURCHASE_COUNT)                                                      D04_PURCHASE_COUNT,
       SUM(D05_PURCHASE_COUNT)                                                      D05_PURCHASE_COUNT,
       SUM(D06_PURCHASE_COUNT)                                                      D06_PURCHASE_COUNT,
       SUM(D07_PURCHASE_COUNT)                                                      D07_PURCHASE_COUNT,
       SUM(D14_PURCHASE_COUNT)                                                      D14_PURCHASE_COUNT,
       SUM(D30_PURCHASE_COUNT)                                                      D30_PURCHASE_COUNT,
       SUM(D60_PURCHASE_COUNT)                                                      D60_PURCHASE_COUNT,
       SUM(D90_PURCHASE_COUNT)                                                      D90_PURCHASE_COUNT,

       SUM(TOTAL_PURCHASE_REVENUE_GROSS)                                            PURCHASE_REVENUE_GROSS,
       SUM(D01_PURCHASE_REVENUE_GROSS)                                              D01_PURCHASE_REVENUE_GROSS,
       SUM(D02_PURCHASE_REVENUE_GROSS)                                              D02_PURCHASE_REVENUE_GROSS,
       SUM(D03_PURCHASE_REVENUE_GROSS)                                              D03_PURCHASE_REVENUE_GROSS,
       SUM(D04_PURCHASE_REVENUE_GROSS)                                              D04_PURCHASE_REVENUE_GROSS,
       SUM(D05_PURCHASE_REVENUE_GROSS)                                              D05_PURCHASE_REVENUE_GROSS,
       SUM(D06_PURCHASE_REVENUE_GROSS)                                              D06_PURCHASE_REVENUE_GROSS,
       SUM(D07_PURCHASE_REVENUE_GROSS)                                              D07_PURCHASE_REVENUE_GROSS,
       SUM(D14_PURCHASE_REVENUE_GROSS)                                              D14_PURCHASE_REVENUE_GROSS,
       SUM(D30_PURCHASE_REVENUE_GROSS)                                              D30_PURCHASE_REVENUE_GROSS,
       SUM(D60_PURCHASE_REVENUE_GROSS)                                              D60_PURCHASE_REVENUE_GROSS,
       SUM(D90_PURCHASE_REVENUE_GROSS)                                              D90_PURCHASE_REVENUE_GROSS,

       SUM(TOTAL_PURCHASE_REVENUE_AFTER_SHARE)                                      PURCHASE_REVENUE_AFTER_SHARE,
       SUM(D01_PURCHASE_REVENUE_AFTER_SHARE)                                        D01_PURCHASE_REVENUE_AFTER_SHARE,
       SUM(D02_PURCHASE_REVENUE_AFTER_SHARE)                                        D02_PURCHASE_REVENUE_AFTER_SHARE,
       SUM(D03_PURCHASE_REVENUE_AFTER_SHARE)                                        D03_PURCHASE_REVENUE_AFTER_SHARE,
       SUM(D04_PURCHASE_REVENUE_AFTER_SHARE)                                        D04_PURCHASE_REVENUE_AFTER_SHARE,
       SUM(D05_PURCHASE_REVENUE_AFTER_SHARE)                                        D05_PURCHASE_REVENUE_AFTER_SHARE,
       SUM(D06_PURCHASE_REVENUE_AFTER_SHARE)                                        D06_PURCHASE_REVENUE_AFTER_SHARE,
       SUM(D07_PURCHASE_REVENUE_AFTER_SHARE)                                        D07_PURCHASE_REVENUE_AFTER_SHARE,
       SUM(D14_PURCHASE_REVENUE_AFTER_SHARE)                                        D14_PURCHASE_REVENUE_AFTER_SHARE,
       SUM(D30_PURCHASE_REVENUE_AFTER_SHARE)                                        D30_PURCHASE_REVENUE_AFTER_SHARE,
       SUM(D60_PURCHASE_REVENUE_AFTER_SHARE)                                        D60_PURCHASE_REVENUE_AFTER_SHARE,
       SUM(D90_PURCHASE_REVENUE_AFTER_SHARE)                                        D90_PURCHASE_REVENUE_AFTER_SHARE,

       SUM(
               PURCHASE_BY_EVENT_DATE.TOTAL_PURCHASE_REVENUE_AFTER_SHARE_AFTER_VAT) PURCHASE_REVENUE_AFTER_REVENUE_SHARE_AFTER_VAT,
       SUM(
               PURCHASE_BY_EVENT_DATE.D01_PURCHASE_REVENUE_AFTER_SHARE_AFTER_VAT)   D01_PURCHASE_REVENUE_AFTER_SHARE_AFTER_VAT,
       SUM(
               PURCHASE_BY_EVENT_DATE.D02_PURCHASE_REVENUE_AFTER_SHARE_AFTER_VAT)   D02_PURCHASE_REVENUE_AFTER_SHARE_AFTER_VAT,
       SUM(
               PURCHASE_BY_EVENT_DATE.D03_PURCHASE_REVENUE_AFTER_SHARE_AFTER_VAT)   D03_PURCHASE_REVENUE_AFTER_SHARE_AFTER_VAT,
       SUM(
               PURCHASE_BY_EVENT_DATE.D04_PURCHASE_REVENUE_AFTER_SHARE_AFTER_VAT)   D04_PURCHASE_REVENUE_AFTER_SHARE_AFTER_VAT,
       SUM(
               PURCHASE_BY_EVENT_DATE.D05_PURCHASE_REVENUE_AFTER_SHARE_AFTER_VAT)   D05_PURCHASE_REVENUE_AFTER_SHARE_AFTER_VAT,
       SUM(
               PURCHASE_BY_EVENT_DATE.D06_PURCHASE_REVENUE_AFTER_SHARE_AFTER_VAT)   D06_PURCHASE_REVENUE_AFTER_SHARE_AFTER_VAT,
       SUM(
               PURCHASE_BY_EVENT_DATE.D07_PURCHASE_REVENUE_AFTER_SHARE_AFTER_VAT)   D07_PURCHASE_REVENUE_AFTER_SHARE_AFTER_VAT,
       SUM(
               PURCHASE_BY_EVENT_DATE.D14_PURCHASE_REVENUE_AFTER_SHARE_AFTER_VAT)   D14_PURCHASE_REVENUE_AFTER_SHARE_AFTER_VAT,
       SUM(
               PURCHASE_BY_EVENT_DATE.D30_PURCHASE_REVENUE_AFTER_SHARE_AFTER_VAT)   D30_PURCHASE_REVENUE_AFTER_SHARE_AFTER_VAT,
       SUM(
               PURCHASE_BY_EVENT_DATE.D60_PURCHASE_REVENUE_AFTER_SHARE_AFTER_VAT)   D60_PURCHASE_REVENUE_AFTER_SHARE_AFTER_VAT,
       SUM(
               PURCHASE_BY_EVENT_DATE.D90_PURCHASE_REVENUE_AFTER_SHARE_AFTER_VAT)   D90_PURCHASE_REVENUE_AFTER_SHARE_AFTER_VAT,

       CURRENT_TIMESTAMP::TIMESTAMP_NTZ                                             INSERT_TIME,
       INSTALL_TIME::TIMESTAMP_NTZ                                                  _TST


FROM PURCHASE_BY_EVENT_DATE
GROUP BY INSTALL_TIME, APPSFLYER_DEVICE_ID
;

COMMIT;
