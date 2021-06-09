CREATE OR REPLACE TABLE KG_DWH.L2_MARKETING_COHORT_MODEL.PURCHASE_COUNT_REVENUE AS

WITH PURCHASE_BY_EVENT_DATE AS (SELECT TO_DATE(INSTALL_TIME)                                                            INSTALL_DATE,
                                       TO_DATE(EVENT_TIME)                                                              EVENT_DATE,
                                       MEDIA_SOURCE,
                                       COUNTRY_CODE,
                                       CAMPAIGN_ID,
                                       SUB_CAMPAIGN_ID,
                                       COUNT(*)                                                                         PURCHASE_COUNT,
                                       COUNT(DISTINCT APPSFLYER_DEVICE_ID)                                              UNIQUE_PURCHASERS,
                                       SUM(PURCHASE_REVENUE_AFTER_SHARE)                                                TOTAL_PURCHASE_REVENUE_AFTER_SHARE,
                                       SUM(PURCHASE_REVENUE_AFTER_SHARE_AFTER_VAT)                                      TOTAL_PURCHASE_REVENUE_AFTER_SHARE_AFTER_VAT,
                                       SUM(PURCHASE_REVENUE_GROSS)                                                      TOTAL_PURCHASE_REVENUE_GROSS,


                                       COUNT(
                                               IFF(DAYS_SINCE_INSTALL < 1, REVENUE_IN_SELECTED_CURRENCY, NULL))         D01_PURCHASE_COUNT,
                                       COUNT(
                                               IFF(DAYS_SINCE_INSTALL < 3, REVENUE_IN_SELECTED_CURRENCY, NULL))         D03_PURCHASE_COUNT,
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
                                       COUNT(
                                               IFF(DAYS_SINCE_INSTALL < 180, REVENUE_IN_SELECTED_CURRENCY, NULL))       D180_PURCHASE_COUNT,
                                       COUNT(
                                               IFF(DAYS_SINCE_INSTALL < 360, REVENUE_IN_SELECTED_CURRENCY, NULL))       D360_PURCHASE_COUNT,

                                       SUM(
                                               IFF(DAYS_SINCE_INSTALL < 1, PURCHASE_REVENUE_GROSS, 0))                  D01_PURCHASE_REVENUE_GROSS,
                                       SUM(
                                               IFF(DAYS_SINCE_INSTALL < 3, PURCHASE_REVENUE_GROSS, 0))                  D03_PURCHASE_REVENUE_GROSS,
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
                                       SUM(
                                               IFF(DAYS_SINCE_INSTALL < 180, PURCHASE_REVENUE_GROSS, 0))                D180_PURCHASE_REVENUE_GROSS,
                                       SUM(
                                               IFF(DAYS_SINCE_INSTALL < 360, PURCHASE_REVENUE_GROSS, 0))                D360_PURCHASE_REVENUE_GROSS,


                                       SUM(IFF(DAYS_SINCE_INSTALL < 1, PURCHASE_REVENUE_AFTER_SHARE, 0))                D01_PURCHASE_REVENUE_AFTER_SHARE,
                                       SUM(IFF(DAYS_SINCE_INSTALL < 3, PURCHASE_REVENUE_AFTER_SHARE, 0))                D03_PURCHASE_REVENUE_AFTER_SHARE,
                                       SUM(IFF(DAYS_SINCE_INSTALL < 7, PURCHASE_REVENUE_AFTER_SHARE, 0))                D07_PURCHASE_REVENUE_AFTER_SHARE,
                                       SUM(IFF(DAYS_SINCE_INSTALL < 14, PURCHASE_REVENUE_AFTER_SHARE, 0))               D14_PURCHASE_REVENUE_AFTER_SHARE,
                                       SUM(IFF(DAYS_SINCE_INSTALL < 30, PURCHASE_REVENUE_AFTER_SHARE, 0))               D30_PURCHASE_REVENUE_AFTER_SHARE,
                                       SUM(IFF(DAYS_SINCE_INSTALL < 60, PURCHASE_REVENUE_AFTER_SHARE, 0))               D60_PURCHASE_REVENUE_AFTER_SHARE,
                                       SUM(IFF(DAYS_SINCE_INSTALL < 90, PURCHASE_REVENUE_AFTER_SHARE, 0))               D90_PURCHASE_REVENUE_AFTER_SHARE,
                                       SUM(IFF(DAYS_SINCE_INSTALL < 180, PURCHASE_REVENUE_AFTER_SHARE, 0))              D180_PURCHASE_REVENUE_AFTER_SHARE,
                                       SUM(IFF(DAYS_SINCE_INSTALL < 360, PURCHASE_REVENUE_AFTER_SHARE, 0))              D360_PURCHASE_REVENUE_AFTER_SHARE,

                                       SUM(
                                               IFF(DAYS_SINCE_INSTALL < 1, PURCHASE_REVENUE_AFTER_SHARE_AFTER_VAT, 0))  D01_PURCHASE_REVENUE_AFTER_SHARE_AFTER_VAT,
                                       SUM(
                                               IFF(DAYS_SINCE_INSTALL < 3, PURCHASE_REVENUE_AFTER_SHARE_AFTER_VAT, 0))  D03_PURCHASE_REVENUE_AFTER_SHARE_AFTER_VAT,
                                       SUM(
                                               IFF(DAYS_SINCE_INSTALL < 7, PURCHASE_REVENUE_AFTER_SHARE_AFTER_VAT, 0))  D07_PURCHASE_REVENUE_AFTER_SHARE_AFTER_VAT,
                                       SUM(
                                               IFF(DAYS_SINCE_INSTALL < 14, PURCHASE_REVENUE_AFTER_SHARE_AFTER_VAT, 0)) D14_PURCHASE_REVENUE_AFTER_SHARE_AFTER_VAT,
                                       SUM(
                                               IFF(DAYS_SINCE_INSTALL < 30, PURCHASE_REVENUE_AFTER_SHARE_AFTER_VAT, 0)) D30_PURCHASE_REVENUE_AFTER_SHARE_AFTER_VAT,
                                       SUM(
                                               IFF(DAYS_SINCE_INSTALL < 60, PURCHASE_REVENUE_AFTER_SHARE_AFTER_VAT, 0)) D60_PURCHASE_REVENUE_AFTER_SHARE_AFTER_VAT,
                                       SUM(
                                               IFF(DAYS_SINCE_INSTALL < 90, PURCHASE_REVENUE_AFTER_SHARE_AFTER_VAT, 0)) D90_PURCHASE_REVENUE_AFTER_SHARE_AFTER_VAT,
                                       SUM(
                                               IFF(DAYS_SINCE_INSTALL < 180, PURCHASE_REVENUE_AFTER_SHARE_AFTER_VAT,
                                                   0))                                                                  D180_PURCHASE_REVENUE_AFTER_SHARE_AFTER_VAT,
                                       SUM(
                                               IFF(DAYS_SINCE_INSTALL < 360, PURCHASE_REVENUE_AFTER_SHARE_AFTER_VAT,
                                                   0))                                                                  D360_PURCHASE_REVENUE_AFTER_SHARE_AFTER_VAT,

                                       CURRENT_TIMESTAMP::TIMESTAMP_NTZ                                                 INSERT_TIME,
                                       INSTALL_DATE::TIMESTAMP_NTZ                                                      _TST
                                FROM "KG_DWH"."L2_APPSFLYER_PUSH_API_EVENT"."APPSFLYER_IN_APP_PURCHASE_WITH_EXCHANGE_RATE_AND_VAT"
                                GROUP BY INSTALL_DATE, EVENT_DATE, MEDIA_SOURCE, COUNTRY_CODE, CAMPAIGN_ID,
                                         SUB_CAMPAIGN_ID)

SELECT INSTALL_DATE::DATE INSTALL_DATE,
       MEDIA_SOURCE::VARCHAR MEDIA_SOURCE,
       PURCHASE_BY_EVENT_DATE.COUNTRY_CODE::VARCHAR COUNTRY_CODE,
       CAMPAIGN_ID::VARCHAR CAMPAIGN_ID,
       SUB_CAMPAIGN_ID::VARCHAR SUB_CAMPAIGN_ID,

       SUM(PURCHASE_COUNT)::BIGINT                              PURCHASE_COUNT,
       SUM(UNIQUE_PURCHASERS)::BIGINT                           UNIQUE_PURCHASERS,
       SUM(D01_PURCHASE_COUNT)::BIGINT                          D01_PURCHASE_COUNT,
       SUM(D03_PURCHASE_COUNT)::BIGINT                          D03_PURCHASE_COUNT,
       SUM(D07_PURCHASE_COUNT)::BIGINT                          D07_PURCHASE_COUNT,
       SUM(D14_PURCHASE_COUNT)::BIGINT                          D14_PURCHASE_COUNT,
       SUM(D30_PURCHASE_COUNT)::BIGINT                          D30_PURCHASE_COUNT,
       SUM(D60_PURCHASE_COUNT)::BIGINT                          D60_PURCHASE_COUNT,
       SUM(D90_PURCHASE_COUNT)::BIGINT                          D90_PURCHASE_COUNT,
       SUM(D180_PURCHASE_COUNT)::BIGINT                         D180_PURCHASE_COUNT,
       SUM(D360_PURCHASE_COUNT)::BIGINT                         D360_PURCHASE_COUNT,

       SUM(TOTAL_PURCHASE_REVENUE_GROSS)::FLOAT                 PURCHASE_REVENUE_GROSS,
       SUM(D01_PURCHASE_REVENUE_GROSS)::FLOAT                   D01_PURCHASE_REVENUE_GROSS,
       SUM(D03_PURCHASE_REVENUE_GROSS)::FLOAT                   D03_PURCHASE_REVENUE_GROSS,
       SUM(D07_PURCHASE_REVENUE_GROSS)::FLOAT                   D07_PURCHASE_REVENUE_GROSS,
       SUM(D14_PURCHASE_REVENUE_GROSS)::FLOAT                   D14_PURCHASE_REVENUE_GROSS,
       SUM(D30_PURCHASE_REVENUE_GROSS)::FLOAT                   D30_PURCHASE_REVENUE_GROSS,
       SUM(D60_PURCHASE_REVENUE_GROSS)::FLOAT                   D60_PURCHASE_REVENUE_GROSS,
       SUM(D90_PURCHASE_REVENUE_GROSS)::FLOAT                   D90_PURCHASE_REVENUE_GROSS,
       SUM(D180_PURCHASE_REVENUE_GROSS)::FLOAT                  D180_PURCHASE_REVENUE_GROSS,
       SUM(D360_PURCHASE_REVENUE_GROSS)::FLOAT                  D360_PURCHASE_REVENUE_GROSS,

       SUM(TOTAL_PURCHASE_REVENUE_AFTER_SHARE)::FLOAT           PURCHASE_REVENUE_AFTER_SHARE,
       SUM(D01_PURCHASE_REVENUE_AFTER_SHARE)::FLOAT             D01_PURCHASE_REVENUE_AFTER_SHARE,
       SUM(D03_PURCHASE_REVENUE_AFTER_SHARE)::FLOAT             D03_PURCHASE_REVENUE_AFTER_SHARE,
       SUM(D07_PURCHASE_REVENUE_AFTER_SHARE)::FLOAT             D07_PURCHASE_REVENUE_AFTER_SHARE,
       SUM(D14_PURCHASE_REVENUE_AFTER_SHARE)::FLOAT             D14_PURCHASE_REVENUE_AFTER_SHARE,
       SUM(D30_PURCHASE_REVENUE_AFTER_SHARE)::FLOAT             D30_PURCHASE_REVENUE_AFTER_SHARE,
       SUM(D60_PURCHASE_REVENUE_AFTER_SHARE)::FLOAT             D60_PURCHASE_REVENUE_AFTER_SHARE,
       SUM(D90_PURCHASE_REVENUE_AFTER_SHARE)::FLOAT             D90_PURCHASE_REVENUE_AFTER_SHARE,
       SUM(D180_PURCHASE_REVENUE_AFTER_SHARE)::FLOAT            D180_PURCHASE_REVENUE_AFTER_SHARE,
       SUM(D360_PURCHASE_REVENUE_AFTER_SHARE)::FLOAT            D360_PURCHASE_REVENUE_AFTER_SHARE,

       SUM(TOTAL_PURCHASE_REVENUE_AFTER_SHARE_AFTER_VAT)::FLOAT PURCHASE_REVENUE_AFTER_SHARE_AFTER_VAT,
       SUM(D01_PURCHASE_REVENUE_AFTER_SHARE_AFTER_VAT)::FLOAT   D01_PURCHASE_REVENUE_AFTER_SHARE_AFTER_VAT,
       SUM(D03_PURCHASE_REVENUE_AFTER_SHARE_AFTER_VAT)::FLOAT   D03_PURCHASE_REVENUE_AFTER_SHARE_AFTER_VAT,
       SUM(D07_PURCHASE_REVENUE_AFTER_SHARE_AFTER_VAT)::FLOAT   D07_PURCHASE_REVENUE_AFTER_SHARE_AFTER_VAT,
       SUM(D14_PURCHASE_REVENUE_AFTER_SHARE_AFTER_VAT)::FLOAT   D14_PURCHASE_REVENUE_AFTER_SHARE_AFTER_VAT,
       SUM(D30_PURCHASE_REVENUE_AFTER_SHARE_AFTER_VAT)::FLOAT   D30_PURCHASE_REVENUE_AFTER_SHARE_AFTER_VAT,
       SUM(D60_PURCHASE_REVENUE_AFTER_SHARE_AFTER_VAT)::FLOAT   D60_PURCHASE_REVENUE_AFTER_SHARE_AFTER_VAT,
       SUM(D90_PURCHASE_REVENUE_AFTER_SHARE_AFTER_VAT)::FLOAT   D90_PURCHASE_REVENUE_AFTER_SHARE_AFTER_VAT,
       SUM(D180_PURCHASE_REVENUE_AFTER_SHARE_AFTER_VAT)::FLOAT  D180_PURCHASE_REVENUE_AFTER_SHARE_AFTER_VAT,
       SUM(D360_PURCHASE_REVENUE_AFTER_SHARE_AFTER_VAT)::FLOAT  D360_PURCHASE_REVENUE_AFTER_SHARE_AFTER_VAT,

       CURRENT_TIMESTAMP::TIMESTAMP_NTZ                         INSERT_TIME,
       INSTALL_DATE::TIMESTAMP_NTZ                              _TST
FROM PURCHASE_BY_EVENT_DATE

GROUP BY INSTALL_DATE, MEDIA_SOURCE, COUNTRY_CODE, CAMPAIGN_ID, SUB_CAMPAIGN_ID
;

