CREATE OR REPLACE TABLE L2_MARKETING_COHORT_MODEL.RETARGETING AS
    (WITH RETARGETING_COST AS (
        SELECT COST.DATE,
               COST.MEDIA_SOURCE,
               COST.COUNTRY_CODE,
               COST.CAMPAIGN_ID,
               COST.SUB_CAMPAIGN_ID,
               COST
        FROM L2_MARKETING_COHORT_MODEL.MARKETING_COST COST
                 LEFT JOIN L2_MARKETING_COHORT_MODEL.CAMPAIGN_INFORMATION INFO
                           ON COST.MEDIA_SOURCE = INFO.MEDIA_SOURCE
                               AND COST.CAMPAIGN_ID = INFO.CAMPAIGN_ID
                               AND COST.SUB_CAMPAIGN_ID = INFO.SUB_CAMPAIGN_ID
        WHERE INFO.CAMPAIGN_NAME LIKE '%-RT-%'),

          RETARGETING_INSTALLS AS (
              SELECT CAST(INSTALL_TIME AS DATE) INSTALL_DATE,
                     MEDIA_SOURCE,
                     COUNTRY_CODE,
                     CAMPAIGN_ID,
                     SUB_CAMPAIGN_ID,
                     SUM(CASE
                             WHEN RE_TARGETING_CONVERSION_TYPE = 're-attribution' THEN 1
                             ELSE 0 END)        RE_ATTRIBUTION_COUNT,
                     SUM(CASE
                             WHEN RE_TARGETING_CONVERSION_TYPE = 're-engagement' THEN 1
                             ELSE 0 END)        RE_ENGAGEMENT_COUNT
              FROM L1_APPSFLYER_PUSH_API_RETARGETING_EVENT.APPSFLYER_INSTALL_RETARGETING
              GROUP BY INSTALL_DATE, MEDIA_SOURCE, COUNTRY_CODE, CAMPAIGN_ID, SUB_CAMPAIGN_ID),

          RETARGETING_PURCHASE AS (
              SELECT CAST(INSTALL_TIME AS DATE)                  INSTALL_DATE,
                     MEDIA_SOURCE,
                     COUNTRY_CODE,
                     CAMPAIGN_ID,
                     SUB_CAMPAIGN_ID,
                     sum(PURCHASE_REVENUE_AFTER_SHARE_AFTER_VAT) RETARGETING_PURCHASE_REVENUE
              FROM L2_APPSFLYER_PUSH_API_RETARGETING_EVENT.APPSFLYER_IN_APP_PURCHASE_WITH_EXCHANGE_RATE_AND_VAT_RETARGETING
              GROUP BY INSTALL_DATE, MEDIA_SOURCE, COUNTRY_CODE, CAMPAIGN_ID, SUB_CAMPAIGN_ID),

          RETARGETING_REWARD_COLLECTED AS
              (SELECT CAST(INSTALL_TIME AS DATE) INSTALL_DATE,
                      MEDIA_SOURCE,
                      COUNTRY_CODE,
                      CAMPAIGN_ID,
                      SUB_CAMPAIGN_ID,
                      COUNT(*)                   RETARGETING_REWARD_COLLECTED
               FROM L1_APPSFLYER_PUSH_API_RETARGETING_EVENT.APPSFLYER_CHURNED_REWARD_COLLECTED_RETARGETING REWARD
               GROUP BY INSTALL_DATE, MEDIA_SOURCE, COUNTRY_CODE, CAMPAIGN_ID, SUB_CAMPAIGN_ID)

     SELECT COALESCE(INSTALL.INSTALL_DATE, COST.DATE, PURCHASE.INSTALL_DATE, REWARD.INSTALL_DATE)::DATE INSTALL_DATE,
            COALESCE(INSTALL.MEDIA_SOURCE, COST.MEDIA_SOURCE, PURCHASE.MEDIA_SOURCE,
                     REWARD.MEDIA_SOURCE)::VARCHAR                                                      MEDIA_SOURCE,
            COALESCE(INSTALL.COUNTRY_CODE, COST.COUNTRY_CODE, PURCHASE.COUNTRY_CODE,
                     REWARD.COUNTRY_CODE)::VARCHAR                                                      COUNTRY_CODE,
            COALESCE(INSTALL.CAMPAIGN_ID, COST.CAMPAIGN_ID, PURCHASE.CAMPAIGN_ID,
                     REWARD.CAMPAIGN_ID)::VARCHAR                                                       CAMPAIGN_ID,
            COALESCE(INSTALL.SUB_CAMPAIGN_ID, COST.SUB_CAMPAIGN_ID, PURCHASE.SUB_CAMPAIGN_ID,
                     REWARD.SUB_CAMPAIGN_ID)::VARCHAR                                                   SUB_CAMPAIGN_ID,
            INFO.CAMPAIGN_NAME::VARCHAR                                                                 CAMPAIGN_NAME,
            INFO.SUB_CAMPAIGN_NAME::VARCHAR                                                             SUB_CAMPAIGN_NAME,
            INFO.GAME_NAME::VARCHAR                                                                     GAME_NAME,
            INFO.PLATFORM::VARCHAR                                                                      PLATFORM,
            INFO.SOURCE::VARCHAR                                                                        SOURCE,
            INSTALL.RE_ATTRIBUTION_COUNT::VARCHAR                                                       RE_ATTRIBUTION_COUNT,
            INSTALL.RE_ENGAGEMENT_COUNT::VARCHAR                                                        RE_ENGAGEMENT_COUNT,
            COST.COST::FLOAT                                                                            RETARGETING_COST,
            REWARD.RETARGETING_REWARD_COLLECTED::NUMBER                                                 RETARGETING_REWARD_COLLECTED,
            PURCHASE.RETARGETING_PURCHASE_REVENUE::FLOAT                                                RETARGETING_PURCHASE_REVENUE,
            CURRENT_TIMESTAMP::TIMESTAMP_NTZ                                                            INSERT_TIME,
            COALESCE(INSTALL.INSTALL_DATE, COST.DATE, PURCHASE.INSTALL_DATE, REWARD.INSTALL_DATE)::TIMESTAMP_NTZ                                                                 _TST

     FROM RETARGETING_INSTALLS INSTALL
              FULL JOIN RETARGETING_COST COST
                        ON INSTALL.INSTALL_DATE = COST.DATE
                            AND INSTALL.MEDIA_SOURCE = COST.MEDIA_SOURCE
                            AND INSTALL.COUNTRY_CODE = COST.COUNTRY_CODE
                            AND INSTALL.CAMPAIGN_ID = COST.CAMPAIGN_ID
                            AND INSTALL.SUB_CAMPAIGN_ID = COST.SUB_CAMPAIGN_ID

              FULL JOIN RETARGETING_PURCHASE PURCHASE
                        ON INSTALL.INSTALL_DATE = PURCHASE.INSTALL_DATE
                            AND INSTALL.MEDIA_SOURCE = PURCHASE.MEDIA_SOURCE
                            AND INSTALL.COUNTRY_CODE = PURCHASE.COUNTRY_CODE
                            AND INSTALL.CAMPAIGN_ID = PURCHASE.CAMPAIGN_ID
                            AND INSTALL.SUB_CAMPAIGN_ID = PURCHASE.SUB_CAMPAIGN_ID

              FULL JOIN RETARGETING_REWARD_COLLECTED REWARD
                        ON INSTALL.INSTALL_DATE = REWARD.INSTALL_DATE
                            AND INSTALL.MEDIA_SOURCE = REWARD.MEDIA_SOURCE
                            AND INSTALL.COUNTRY_CODE = REWARD.COUNTRY_CODE
                            AND INSTALL.CAMPAIGN_ID = REWARD.CAMPAIGN_ID
                            AND INSTALL.SUB_CAMPAIGN_ID = REWARD.SUB_CAMPAIGN_ID

              LEFT JOIN L2_MARKETING_COHORT_MODEL.CAMPAIGN_INFORMATION INFO
                        ON COALESCE(INSTALL.MEDIA_SOURCE, COST.MEDIA_SOURCE, PURCHASE.MEDIA_SOURCE,
                                    REWARD.MEDIA_SOURCE) = INFO.MEDIA_SOURCE
                            AND
                           COALESCE(INSTALL.CAMPAIGN_ID, COST.CAMPAIGN_ID, PURCHASE.CAMPAIGN_ID, REWARD.CAMPAIGN_ID) =
                           INFO.CAMPAIGN_ID
                            AND COALESCE(INSTALL.SUB_CAMPAIGN_ID, COST.SUB_CAMPAIGN_ID, PURCHASE.SUB_CAMPAIGN_ID,
                                         REWARD.SUB_CAMPAIGN_ID) = INFO.SUB_CAMPAIGN_ID
     WHERE COALESCE(INSTALL.INSTALL_DATE, COST.DATE, PURCHASE.INSTALL_DATE, REWARD.INSTALL_DATE) < CURRENT_DATE()
       AND COALESCE(INSTALL.INSTALL_DATE, COST.DATE, PURCHASE.INSTALL_DATE, REWARD.INSTALL_DATE) >= '2019-11-15'
    )




