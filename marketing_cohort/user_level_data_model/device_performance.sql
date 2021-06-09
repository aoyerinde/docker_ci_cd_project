BEGIN;

CREATE OR REPLACE TABLE L2_MARKETING_USER_LEVEL_MODEL.DEVICE_PERFORMANCE AS

WITH BASE AS (
    SELECT INSTALL_DATE,
           GAME_NAME,
           PLATFORM,
           MEDIA_SOURCE,
           (IFF(MEDIA_SOURCE = 'Organic', 'Organic', 'Non-Organic'))   SOURCE,
           COUNTRY_CODE,
           CAMPAIGN_NAME,
           CAMPAIGN_ID,
           OS_VERSION,
           DEVICE_BRAND,
           DEVICE_MODEL,
           COUNT(*)             AS                                     INSTALLS,
           SUM(ZEROIFNULL(CPI)) AS                                     COST,
           SUM(ZEROIFNULL(D01_PURCHASE_REVENUE_AFTER_SHARE_AFTER_VAT)) D01_PURCHASE_REVENUE_AFTER_SHARE_AFTER_VAT,
           SUM(ZEROIFNULL(D03_PURCHASE_REVENUE_AFTER_SHARE_AFTER_VAT)) D03_PURCHASE_REVENUE_AFTER_SHARE_AFTER_VAT,
           SUM(ZEROIFNULL(D07_PURCHASE_REVENUE_AFTER_SHARE_AFTER_VAT)) D07_PURCHASE_REVENUE_AFTER_SHARE_AFTER_VAT,
           SUM(ZEROIFNULL(D14_PURCHASE_REVENUE_AFTER_SHARE_AFTER_VAT))                D14_PURCHASE_REVENUE_AFTER_SHARE_AFTER_VAT,
           SUM(ZEROIFNULL(D30_PURCHASE_REVENUE_AFTER_SHARE_AFTER_VAT))                D30_PURCHASE_REVENUE_AFTER_SHARE_AFTER_VAT,
           SUM(ZEROIFNULL(D60_PURCHASE_REVENUE_AFTER_SHARE_AFTER_VAT))                D60_PURCHASE_REVENUE_AFTER_SHARE_AFTER_VAT,
           SUM(ZEROIFNULL(D90_PURCHASE_REVENUE_AFTER_SHARE_AFTER_VAT))                D90_PURCHASE_REVENUE_AFTER_SHARE_AFTER_VAT,
           SUM(ZEROIFNULL(PURCHASE_REVENUE_AFTER_SHARE_AFTER_VAT))                    PURCHASE_REVENUE_AFTER_SHARE_AFTER_VAT,
           SUM(ZEROIFNULL(D01_AD_WATCHED_REVENUE))                                    D01_AD_WATCHED_REVENUE,
           SUM(ZEROIFNULL(D03_AD_WATCHED_REVENUE))                                    D03_AD_WATCHED_REVENUE,
           SUM(ZEROIFNULL(D07_AD_WATCHED_REVENUE))                                    D07_AD_WATCHED_REVENUE,
           SUM(ZEROIFNULL(D14_AD_WATCHED_REVENUE))                                    D14_AD_WATCHED_REVENUE,
           SUM(ZEROIFNULL(D30_AD_WATCHED_REVENUE))                                    D30_AD_WATCHED_REVENUE,
           SUM(ZEROIFNULL(D60_AD_WATCHED_REVENUE))                                    D60_AD_WATCHED_REVENUE,
           SUM(ZEROIFNULL(D90_AD_WATCHED_REVENUE))                                    D90_AD_WATCHED_REVENUE,
           SUM(ZEROIFNULL(AD_WATCHED_REVENUE))                                        AD_WATCHED_REVENUE,
           COALESCE(PRED_COUNTRY_TIER_TBL.COUNTRY_TIER, 'ROW')                        PRED_COUNTRY_TIER,
           COALESCE(PERFORMANCE_TARGET_TBL.COUNTRY_TIER, 'ROW')                       PERFORMANCE_TARGET_COUNTRY_TIER

    FROM L2_MARKETING_USER_LEVEL_MODEL.MARKETING_USER_LEVEL

             --DYNAMIC COUNTRY TIER NEEDED FOR PERFORMANCE TARGET JOIN
             LEFT OUTER JOIN (SELECT DISTINCT COUNTRY_TIER
                              FROM L1_MARKETING.STATIC_PERFORMANCE_TARGET) PERFORMANCE_TARGET_TBL
                             ON PERFORMANCE_TARGET_TBL.COUNTRY_TIER = COUNTRY_CODE

        --DYNAMIC COUNTRY TIER NEEDED FOR D90 PREDICTED REVENUE
             LEFT OUTER JOIN (SELECT DISTINCT COUNTRY_TIER
                              FROM L1_MARKETING.STATIC_LTV_PREDICTION_MULTIPLIERS) PRED_COUNTRY_TIER_TBL
                             ON PRED_COUNTRY_TIER_TBL.COUNTRY_TIER = COUNTRY_CODE

    GROUP BY INSTALL_DATE,
             GAME_NAME,
             PLATFORM,
             COUNTRY_CODE,
             MEDIA_SOURCE,
             SOURCE,
             CAMPAIGN_NAME,
             CAMPAIGN_ID,
             OS_VERSION,
             DEVICE_BRAND,
             DEVICE_MODEL,
             PERFORMANCE_TARGET_COUNTRY_TIER,
             PRED_COUNTRY_TIER
)

SELECT BASE.INSTALL_DATE::DATE                                                                  INSTALL_DATE,
       BASE.GAME_NAME::VARCHAR                                                                  GAME_NAME,
       BASE.PLATFORM::VARCHAR                                                                   PLATFORM,
       BASE.MEDIA_SOURCE::VARCHAR                                                               MEDIA_SOURCE,
       BASE.SOURCE::VARCHAR                                                                     SOURCE,
       BASE.COUNTRY_CODE::VARCHAR                                                               COUNTRY_CODE,
       BASE.CAMPAIGN_NAME::VARCHAR                                                              CAMPAIGN_NAME,
       BASE.CAMPAIGN_ID::VARCHAR                                                                CAMPAIGN_ID,
       BASE.OS_VERSION::VARCHAR                                                                 OS_VERSION,
       BASE.DEVICE_BRAND::VARCHAR                                                               DEVICE_BRAND,
       BASE.DEVICE_MODEL::VARCHAR                                                               DEVICE_MODEL,
       BASE.INSTALLS::NUMBER                                                                    INSTALLS,
       BASE.COST::FLOAT                                                                         COST,
       BASE.D01_PURCHASE_REVENUE_AFTER_SHARE_AFTER_VAT::FLOAT                                   D01_PURCHASE_REVENUE_AFTER_SHARE_AFTER_VAT,
       BASE.D03_PURCHASE_REVENUE_AFTER_SHARE_AFTER_VAT::FLOAT                                   D03_PURCHASE_REVENUE_AFTER_SHARE_AFTER_VAT,
       BASE.D07_PURCHASE_REVENUE_AFTER_SHARE_AFTER_VAT::FLOAT                                   D07_PURCHASE_REVENUE_AFTER_SHARE_AFTER_VAT,
       BASE.D14_PURCHASE_REVENUE_AFTER_SHARE_AFTER_VAT::FLOAT                                   D14_PURCHASE_REVENUE_AFTER_SHARE_AFTER_VAT,
       BASE.D30_PURCHASE_REVENUE_AFTER_SHARE_AFTER_VAT::FLOAT                                   D30_PURCHASE_REVENUE_AFTER_SHARE_AFTER_VAT,
       BASE.D60_PURCHASE_REVENUE_AFTER_SHARE_AFTER_VAT::FLOAT                                   D60_PURCHASE_REVENUE_AFTER_SHARE_AFTER_VAT,
       BASE.D90_PURCHASE_REVENUE_AFTER_SHARE_AFTER_VAT::FLOAT                                   D90_PURCHASE_REVENUE_AFTER_SHARE_AFTER_VAT,
       BASE.PURCHASE_REVENUE_AFTER_SHARE_AFTER_VAT::FLOAT                                       PURCHASE_REVENUE_AFTER_SHARE_AFTER_VAT,
       BASE.D01_AD_WATCHED_REVENUE::FLOAT                                                       D01_AD_WATCHED_REVENUE,
       BASE.D03_AD_WATCHED_REVENUE::FLOAT                                                       D03_AD_WATCHED_REVENUE,
       BASE.D07_AD_WATCHED_REVENUE::FLOAT                                                       D07_AD_WATCHED_REVENUE,
       BASE.D14_AD_WATCHED_REVENUE::FLOAT                                                       D14_AD_WATCHED_REVENUE,
       BASE.D30_AD_WATCHED_REVENUE::FLOAT                                                       D30_AD_WATCHED_REVENUE,
       BASE.D60_AD_WATCHED_REVENUE::FLOAT                                                       D60_AD_WATCHED_REVENUE,
       BASE.D90_AD_WATCHED_REVENUE::FLOAT                                                       D90_AD_WATCHED_REVENUE,
       BASE.AD_WATCHED_REVENUE::FLOAT                                                           AD_WATCHED_REVENUE,

       --D90 PREDICTED REVENUE FINAL MEASURE
       ZEROIFNULL(CASE
                      WHEN DATEDIFF(DAY, BASE.INSTALL_DATE, CURRENT_DATE()) < 3 THEN
                              (ZEROIFNULL(D01_AD_WATCHED_REVENUE) +
                               ZEROIFNULL(D01_PURCHASE_REVENUE_AFTER_SHARE_AFTER_VAT)) * D90_D01
                      WHEN DATEDIFF(DAY, BASE.INSTALL_DATE, CURRENT_DATE()) < 7 THEN
                              (ZEROIFNULL(D03_AD_WATCHED_REVENUE) +
                               ZEROIFNULL(D03_PURCHASE_REVENUE_AFTER_SHARE_AFTER_VAT)) * D90_D03
                      WHEN DATEDIFF(DAY, BASE.INSTALL_DATE, CURRENT_DATE()) < 14 THEN
                              (ZEROIFNULL(D07_AD_WATCHED_REVENUE) +
                               ZEROIFNULL(D07_PURCHASE_REVENUE_AFTER_SHARE_AFTER_VAT)) * D90_D07
                      WHEN DATEDIFF(DAY, BASE.INSTALL_DATE, CURRENT_DATE()) < 30 THEN
                              (ZEROIFNULL(D14_AD_WATCHED_REVENUE) +
                               ZEROIFNULL(D14_PURCHASE_REVENUE_AFTER_SHARE_AFTER_VAT)) * D90_D14
                      WHEN DATEDIFF(DAY, BASE.INSTALL_DATE, CURRENT_DATE()) < 60 THEN
                              (ZEROIFNULL(D30_AD_WATCHED_REVENUE) +
                               ZEROIFNULL(D30_PURCHASE_REVENUE_AFTER_SHARE_AFTER_VAT)) * D90_D30
                      WHEN DATEDIFF(DAY, BASE.INSTALL_DATE, CURRENT_DATE()) < 90 THEN
                              (ZEROIFNULL(D60_AD_WATCHED_REVENUE) +
                               ZEROIFNULL(D60_PURCHASE_REVENUE_AFTER_SHARE_AFTER_VAT)) * D90_D60
                      ELSE (ZEROIFNULL(D90_AD_WATCHED_REVENUE) +
                            ZEROIFNULL(D90_PURCHASE_REVENUE_AFTER_SHARE_AFTER_VAT)) END)::FLOAT PRED_D90_REVENUE,

       ZEROIFNULL(TARGET.PERFORMANCE_TARGET)::FLOAT                                             PERFORMANCE_TARGET,
       CURRENT_TIMESTAMP::TIMESTAMP_NTZ                                                         INSERT_TIME,
       INSTALL_DATE::TIMESTAMP_NTZ                                                              _TST

FROM BASE
         --D90 PREDICTED REVENUE
         LEFT OUTER JOIN L1_MARKETING.STATIC_LTV_PREDICTION_MULTIPLIERS PRED
                         ON (BASE.GAME_NAME = PRED.APP_NAME
                             AND BASE.PLATFORM = PRED.PLATFORM
                             AND BASE.PRED_COUNTRY_TIER = PRED.COUNTRY_TIER
                             AND BASE.SOURCE = PRED.SOURCE)


    --PERFORMANCE TARGET JOIN
         LEFT OUTER JOIN L1_MARKETING.STATIC_PERFORMANCE_TARGET TARGET
                         ON (BASE.GAME_NAME = TARGET.GAME_NAME
                             AND BASE.PLATFORM = TARGET.PLATFORM
                             AND BASE.PERFORMANCE_TARGET_COUNTRY_TIER = TARGET.COUNTRY_TIER);

COMMIT;