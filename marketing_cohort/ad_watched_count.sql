CREATE TABLE IF NOT EXISTS KG_DWH.L2_MARKETING_COHORT_MODEL.AD_WATCHED_COUNT
(
    EVENT_DATE    DATE,
    COUNTRY_CODE    VARCHAR,
    GAME_NAME     VARCHAR,
    PLATFORM     VARCHAR,
    AD_WATCHED_COUNT   BIGINT,
    INSERT_TIME     TIMESTAMP_NTZ,
    _TST            TIMESTAMP_NTZ
);

BEGIN;

TRUNCATE KG_DWH.L2_MARKETING_COHORT_MODEL.AD_WATCHED_COUNT;

INSERT INTO KG_DWH.L2_MARKETING_COHORT_MODEL.AD_WATCHED_COUNT
SELECT TO_DATE(EVENT_TIME)              EVENT_DATE,
       COUNTRY_CODE,
       GAME_NAME,
       PLATFORM,
       COUNT(*)                         AD_WATCHED_COUNT,
       CURRENT_TIMESTAMP::TIMESTAMP_NTZ INSERT_TIME,
       EVENT_DATE::TIMESTAMP_NTZ        _TST
FROM KG_DWH.L1_APPSFLYER_PUSH_API_EVENT.APPSFLYER_AD_WATCHED
GROUP BY EVENT_DATE, COUNTRY_CODE, GAME_NAME, PLATFORM
;

COMMIT;
