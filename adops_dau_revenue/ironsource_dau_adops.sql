BEGIN;


DELETE
FROM {TARGET_TABLE}
WHERE {L1_SLICE}
;

INSERT INTO {TARGET_TABLE}
SELECT DATA:date::DATE,
       DATA:bundleId::VARCHAR,
       DATA:platform::VARCHAR,
       DATA:providerName::VARCHAR,
       DATA:instanceName::VARCHAR,
       DATA:countryCode::VARCHAR,
       DATA:adSourceChecks::FLOAT,
       DATA:adSourceResponses::FLOAT,
       DATA:impressions::FLOAT,
       DATA:revenue::FLOAT
FROM {SOURCE_TABLE}
WHERE {SLICE}
AND DATA:bundleId::VARCHAR = 'com.fluffyfairygames.idlefactorytycoon'
;

COMMIT;
