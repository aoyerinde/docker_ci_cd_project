CREATE TABLE IF NOT EXISTS KG_DWH.L2_MARKETING_USER_LEVEL_MODEL.USER_INSTALL
(
    INSTALL_TIME        TIMESTAMP_NTZ,
    APPSFLYER_DEVICE_ID VARCHAR,
    GAME_NAME           VARCHAR,
    PLATFORM            VARCHAR,
    SOURCE              VARCHAR,
    MEDIA_SOURCE        VARCHAR,
    COUNTRY_CODE        VARCHAR,
    CAMPAIGN_NAME       VARCHAR,
    SUB_CAMPAIGN_NAME   VARCHAR,
    CAMPAIGN_ID         VARCHAR,
    SUB_CAMPAIGN_ID     VARCHAR,
    OS_VERSION          VARCHAR,
    DEVICE_BRAND        VARCHAR,
    DEVICE_MODEL        VARCHAR,
    ADVERTISING_ID      VARCHAR,
    IDFA                VARCHAR,
    AF_AD               VARCHAR,
    AF_AD_ID            VARCHAR,
    AF_AD_TYPE          VARCHAR,
    AF_SUB1             VARCHAR,
    AF_SUB2             VARCHAR,
    AF_SUB3             VARCHAR,
    AF_SUB4             VARCHAR,
    AF_SUB5             VARCHAR,
    AF_KEYWORDS         VARCHAR,
    AF_SITEID           VARCHAR,
    AF_CHANNEL          VARCHAR,
    AF_COST_MODEL       VARCHAR,
    INSTALL_DATE        DATE,
    EVENT_DATE          DATE,
    CPI                 FLOAT,
    INSERT_TIME         TIMESTAMP_NTZ,
    _TST                TIMESTAMP_NTZ,
    LANGUAGE VARCHAR


);

BEGIN;

TRUNCATE TABLE KG_DWH.L2_MARKETING_USER_LEVEL_MODEL.USER_INSTALL;

INSERT INTO KG_DWH.L2_MARKETING_USER_LEVEL_MODEL.USER_INSTALL

SELECT DISTINCT INSTALL.INSTALL_TIME,
                INSTALL.APPSFLYER_DEVICE_ID,
                INSTALL.GAME_NAME,
                INSTALL.PLATFORM,
                INSTALL.SOURCE,
                INSTALL.MEDIA_SOURCE,
                INSTALL.COUNTRY_CODE,
                INSTALL.CAMPAIGN_NAME,
                INSTALL.SUB_CAMPAIGN_NAME,
                INSTALL.CAMPAIGN_ID,
                INSTALL.SUB_CAMPAIGN_ID,
                INSTALL.OS_VERSION,
                INSTALL.DEVICE_BRAND,
                INSTALL.DEVICE_MODEL,
                INSTALL.ADVERTISING_ID,
                INSTALL.IDFA,
                INSTALL.AF_AD,
                INSTALL.AF_AD_ID,
                INSTALL.AF_AD_TYPE,
                INSTALL.AF_SUB1,
                INSTALL.AF_SUB2,
                INSTALL.AF_SUB3,
                INSTALL.AF_SUB4,
                INSTALL.AF_SUB5,
                INSTALL.AF_KEYWORDS,
                INSTALL.AF_SITEID,
                INSTALL.AF_CHANNEL,
                INSTALL.AF_COST_MODEL,
                TO_DATE(INSTALL.INSTALL_TIME)                                            INSTALL_DATE,
                TO_DATE(INSTALL.EVENT_TIME)                                              EVENT_DATE,
                COALESCE(AF_COST_VALUE, (COHORT.COST / NULLIF(COHORT.INSTALL_COUNT, 0))) CPI,
                CURRENT_TIMESTAMP::TIMESTAMP_NTZ                                         INSERT_TIME,
                INSTALL_DATE::TIMESTAMP_NTZ                                              _TST,
                LANGUAGE VARCHAR


FROM L1_APPSFLYER_PUSH_API_EVENT.APPSFLYER_INSTALL INSTALL
         LEFT JOIN L2_MARKETING_COHORT_MODEL.MARKETING_COHORT COHORT
                   ON (
                           TO_DATE(INSTALL.EVENT_TIME) = COHORT.INSTALL_DATE
                           AND INSTALL.MEDIA_SOURCE = COHORT.MEDIA_SOURCE
                           AND INSTALL.COUNTRY_CODE = COHORT.COUNTRY_CODE
                           AND INSTALL.CAMPAIGN_ID = COHORT.CAMPAIGN_ID
                           AND INSTALL.SUB_CAMPAIGN_ID = COHORT.SUB_CAMPAIGN_ID
                       )
;

COMMIT;