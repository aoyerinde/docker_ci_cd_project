BEGIN;

DELETE
FROM {TARGET_TABLE}
WHERE {SLICE}
;


INSERT INTO {TARGET_TABLE}
SELECT
        DATA:date::DATE DATE,
        DATA:os::VARCHAR OS,
        DATA:category::VARCHAR CATEGORY,
        DATA:country::VARCHAR COUNTRY,
        DATA:country_name::VARCHAR COUNTRY_NAME,
        DATA:downloads::INTEGER DOWNLOADS,
        DATA:downloads_share::FLOAT DOWNLOADS_SHARE,
        DATA:first_seen_at::DATE FIRST_SEEN_AT,
        DATA:occurrences::INTEGER OCCURRENCES,
        DATA:type::VARCHAR TYPE,
        DATA:creatives[0]::VARCHAR CREATIVES,
        DATA:feature_downloads[1]::INTEGER FEATURE_DOWNLOADS,
        DATA:feature_downloads[0]::DATE FEATURE_DOWNLOADS_DATE,
        DATA:path[0]::VARCHAR PATH_1,
        DATA:path[1]::VARCHAR PATH_2,
        DATA:path[2]::VARCHAR PATH_3,
        DATA:positions[1]::INTEGER POSITION,
        _TST,
        INSERT_TIME,
        DATA:app_id::VARCHAR APP_ID,
        DATA:game_name::VARCHAR GAME_NAME,
        DATA:game_id::INTEGER GAME_ID
FROM {SOURCE_TABLE}
WHERE {SLICE}
;


COMMIT;
