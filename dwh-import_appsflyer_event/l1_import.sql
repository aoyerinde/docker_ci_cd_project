CREATE TABLE IF NOT EXISTS {TARGET_TABLE} (
  ADVERTISING_ID VARCHAR,
  AF_AD VARCHAR,
  AF_AD_ID VARCHAR,
  AF_AD_TYPE VARCHAR,
  AF_ADSET VARCHAR,
  AF_ADSET_ID VARCHAR,
  AF_C_ID VARCHAR,
  AF_CHANNEL VARCHAR,
  AF_COST_CURRENCY VARCHAR,
  AF_COST_MODEL VARCHAR,
  AF_COST_VALUE FLOAT,
  AF_KEYWORDS VARCHAR,
  AF_SITEID VARCHAR,
  AF_SUB1 VARCHAR,
  AF_SUB2 VARCHAR,
  AF_SUB3 VARCHAR,
  AF_SUB4 VARCHAR,
  AF_SUB5 VARCHAR,
  APP_ID VARCHAR,
  APP_NAME VARCHAR,
  APP_VERSION VARCHAR,
  APPSFLYER_DEVICE_ID VARCHAR,
  ATTRIBUTED_TOUCH_TIME TIMESTAMP_NTZ,
  ATTRIBUTED_TOUCH_TYPE VARCHAR,
  SOURCE VARCHAR,
  BUNDLE_IDENTIFIER VARCHAR,
  CAMPAIGN VARCHAR,
  CITY VARCHAR,
  CLICK_TIME TIMESTAMP_NTZ,
  CURRENCY VARCHAR,
  CUSTOMER_USER_ID VARCHAR,
  DEVICE_BRAND VARCHAR,
  DOWNLOAD_TIME TIMESTAMP_NTZ,
  DEVICE_MODEL VARCHAR,
  EVENT_NAME VARCHAR,
  EVENT_TIME TIMESTAMP_NTZ,
  EVENT_TYPE VARCHAR,
  EVENT_VALUE VARIANT,
  FB_ADGROUP_ID VARCHAR,
  FB_ADGROUP_NAME VARCHAR,
  FB_ADSET_ID VARCHAR,
  FB_ADSET_NAME VARCHAR,
  FB_CAMPAIGN_ID VARCHAR,
  FB_CAMPAIGN_NAME VARCHAR,
  IDFA VARCHAR,
  IDFV VARCHAR,
  INSTALL_TIME TIMESTAMP_NTZ,
  IS_RETARGETING BOOLEAN,
  OS_VERSION VARCHAR,
  RE_TARGETING_CONVERSION_TYPE VARCHAR,
  AGENCY VARCHAR,
  REVENUE_IN_SELECTED_CURRENCY FLOAT,
  SDK_VERSION VARCHAR,
  SELECTED_CURRENCY VARCHAR,
  WIFI BOOLEAN,
  CARRIER VARCHAR,
  GAME_NAME VARCHAR,
  CAMPAIGN_ID VARCHAR,
  SUB_CAMPAIGN_ID VARCHAR,
  CAMPAIGN_NAME VARCHAR,
  SUB_CAMPAIGN_NAME VARCHAR,
  MEDIA_SOURCE VARCHAR,
  PLATFORM VARCHAR,
  COUNTRY_CODE VARCHAR,
  DAYS_SINCE_INSTALL NUMBER,
  USD_REPORTED_COST_PER_INSTALL FLOAT,
  SERVER_TIME TIMESTAMP_NTZ,
  INSERT_TIME TIMESTAMP_NTZ,
  _TST TIMESTAMP_NTZ,
  LANGUAGE VARCHAR
);

BEGIN;

DELETE
FROM {TARGET_TABLE}
WHERE {SLICE};


---Raw to L1
INSERT INTO {TARGET_TABLE}

SELECT advertising_id,
       af_ad,
       af_ad_id,
       af_ad_type,
       af_adset,
       af_adset_id,
       af_c_id,
       af_channel,
       af_cost_currency,
       af_cost_model,
       af_cost_value,
       af_keywords,
       af_siteid,
       af_sub1,
       af_sub2,
       af_sub3,
       af_sub4,
       af_sub5,
       app_id,
       app_name,
       app_version,
       appsflyer_device_id,
       attributed_touch_time,
       attributed_touch_type,
       source,
       MAIN.bundle_identifier,
       campaign,
       city,
       click_time,
       currency,
       MAIN.customer_user_id,
       device_brand,
       download_time,
       device_model,
       event_name,
       event_time,
       event_type,
       event_value,
       fb_adgroup_id,
       fb_adgroup_name,
       fb_adset_id,
       fb_adset_name,
       fb_campaign_id,
       fb_campaign_name,
       idfa,
       idfv,
       install_time,
       is_retargeting,
       os_version,
       re_targeting_conversion_type,
       agency,
       revenue_in_selected_currency,
       sdk_version,
       selected_currency,
       wifi,
       carrier,
       MAIN.GAME_NAME,
       Campaign_Id,
       Sub_Campaign_Id,
       Campaign_Name,
       Sub_Campaign_Name,
       Media_Source,
       PLATFORM,
       COUNTRY.COUNTRY_CODE COUNTRY_CODE,
       days_since_install,
       usd_reported_cost_per_install,
       SERVER_TIME,
       CURRENT_TIMESTAMP::TIMESTAMP_NTZ,
       MAIN._TST,
       language
FROM
    (
    WITH FLAT AS ( SELECT
        EVENT:advertising_id::VARCHAR advertising_id,
        EVENT:af_ad::VARCHAR af_ad,
        EVENT:af_ad_id::VARCHAR af_ad_id,
        EVENT:af_ad_type::VARCHAR af_ad_type,
        EVENT:af_adset::VARCHAR af_adset,
        EVENT:af_adset_id::VARCHAR af_adset_id,
        EVENT:af_c_id::VARCHAR af_c_id,
        EVENT:af_channel::VARCHAR af_channel,
        EVENT:af_cost_currency::VARCHAR af_cost_currency,
        EVENT:af_cost_model::VARCHAR af_cost_model,
        EVENT:af_cost_value::FLOAT af_cost_value,
        EVENT:af_keywords::VARCHAR af_keywords,
        EVENT:af_siteid::VARCHAR af_siteid,
        EVENT:af_sub1::VARCHAR af_sub1,
        EVENT:af_sub2::VARCHAR af_sub2,
        EVENT:af_sub3::VARCHAR af_sub3,
        EVENT:af_sub4::VARCHAR af_sub4,
        EVENT:af_sub5::VARCHAR af_sub5,
        COALESCE(EVENT:agency::VARCHAR, EVENT:af_prt::VARCHAR) agency,
        EVENT:app_id::VARCHAR app_id,
        EVENT:app_name::VARCHAR app_name,
        EVENT:app_version::VARCHAR app_version,
        COALESCE(EVENT:appsflyer_device_id::VARCHAR, EVENT:appsflyer_id::VARCHAR) appsflyer_device_id,
        EVENT:attributed_touch_time::TIMESTAMP attributed_touch_time,
        EVENT:attributed_touch_type::VARCHAR attributed_touch_type,
        EVENT:bundle_id::VARCHAR BUNDLE_IDENTIFIER,
        EVENT:campaign::VARCHAR campaign,
        EVENT:city::VARCHAR city,
        COALESCE(EVENT:click_time::TIMESTAMP, EVENT:attributed_touch_time::TIMESTAMP)  click_time,
        EVENT:country_code::VARCHAR rawcountrycode,
        COALESCE(EVENT:currency::VARCHAR, EVENT:af_cost_currency::VARCHAR) currency,
        EVENT:customer_user_id::VARCHAR customer_user_id,
        EVENT:device_model::VARCHAR device_model,
        EVENT:device_type::VARCHAR device_type,
        EVENT:device_brand::VARCHAR device_brand,
        COALESCE(EVENT:download_time::TIMESTAMP,EVENT:device_download_time::TIMESTAMP) download_time,
        EVENT:event_name::VARCHAR event_name,
        EVENT:event_time::TIMESTAMP event_time,
        IFF(EVENT:event_name::VARCHAR IN ('install', 're-attribution', 're-engagement'), 'install', 'in-app-event') event_type,
        TRY_CAST(SPLIT_PART(PARSE_JSON(EVENT:event_value)['for_testing'], '"', 0) AS BOOLEAN) for_testing,
        TO_VARIANT(PARSE_JSON(EVENT:event_value)) event_value,
        EVENT:fb_adgroup_id::VARCHAR fb_adgroup_id,
        EVENT:fb_adgroup_name::VARCHAR fb_adgroup_name,
        EVENT:fb_adset_id::VARCHAR fb_adset_id,
        EVENT:fb_adset_name::VARCHAR fb_adset_name,
        EVENT:fb_campaign_id::VARCHAR fb_campaign_id,
        EVENT:fb_campaign_name::VARCHAR fb_campaign_name,
        EVENT:idfa::VARCHAR idfa,
        EVENT:idfv::VARCHAR idfv,
        EVENT:install_time::TIMESTAMP install_time,
        EVENT:is_retargeting::BOOLEAN is_retargeting,
        LOWER(EVENT:media_source::VARCHAR) media_source,
        IFF(media_source = 'organic', 'organic', 'regular') attribution_type,
        EVENT:os_version::VARCHAR os_version,
        EVENT:platform::VARCHAR rawplatform,
        COALESCE (EVENT:re_targeting_conversion_type::VARCHAR, EVENT:retargeting_conversion_type::VARCHAR) re_targeting_conversion_type,
        EVENT:revenue_in_selected_currency::FLOAT revenue_in_selected_currency,
        EVENT:sdk_version::VARCHAR sdk_version,
        EVENT:selected_currency::VARCHAR selected_currency,
        EVENT:wifi::BOOLEAN wifi,
        EVENT:carrier::VARCHAR carrier,
        EVENT:campaign_name::VARCHAR rawcampaignname,
        LOWER(EVENT:language::VARCHAR) language,
        SERVER_TIME,
        _Tst,
        GAME_NAME
FROM
 {SOURCE_TABLE} A
 LEFT JOIN KG_DWH.L1_REFERENCE_DATA.AVAILABLE_APPSFLYER_GAMES_IN_L1 GAMES
 ON A.EVENT:bundle_id::varchar = GAMES.BUNDLE_IDENTIFIER)

SELECT advertising_id,
       af_ad,
       af_ad_id,
       af_ad_type,
       af_adset,
       af_adset_id,
       af_c_id,
       af_channel,
       af_cost_currency,
       af_cost_model,
       af_cost_value,
       af_keywords,
       af_siteid,
       af_sub1,
       af_sub2,
       af_sub3,
       af_sub4,
       af_sub5,
       app_id,
       app_name,
       app_version,
       appsflyer_device_id,
       attributed_touch_time,
       attributed_touch_type,
       bundle_identifier,
       campaign,
       city,
       click_time,
       currency,
       customer_user_id,
       device_type,
       agency,
       download_time,
       event_name,
       event_time,
       event_type,
       event_value,
       fb_adgroup_id,
       fb_adgroup_name,
       fb_adset_id,
       fb_adset_name,
       fb_campaign_id,
       fb_campaign_name,
       idfa,
       idfv,
       install_time,
       is_retargeting,
       os_version,
       re_targeting_conversion_type,
       for_testing,
       revenue_in_selected_currency,
       sdk_version,
       selected_currency,
       wifi,
       carrier,
       GAME_NAME,

      COALESCE(CASE WHEN attribution_type = 'regular' THEN 'Non-Organic'
           WHEN attribution_type = 'organic' THEN 'Organic'
           WHEN media_source = 'restricted' THEN 'Non-Organic'
           END, 'None') SOURCE,


      COALESCE(
        CASE WHEN rawplatform = 'ios' THEN 'iOS'
           WHEN rawplatform = 'android' THEN 'Android'
           ELSE rawplatform
        END,
        'None')
      AS PLATFORM,


      IFF(rawplatform = 'android', coalesce(device_brand, split_part(device_type, '::', 0)), 'Apple')::VARCHAR device_brand,
      IFF(rawplatform = 'android', coalesce(device_model, split_part(device_type, '::', 2)), device_type)::VARCHAR device_model,

     COALESCE(
        CASE WHEN media_source = 'organic' THEN CONCAT(GAME_NAME, ' ', PLATFORM , ' ', SOURCE)
           WHEN media_source = 'beeswax_int' THEN af_adset_id
           WHEN media_source = 'facebook ads' THEN coalesce(fb_campaign_id, af_c_id)
           WHEN agency = 'Mobvista' THEN campaign
           WHEN af_channel = 'Kayzen' THEN SPLIT(af_adset_id,'|')[0]
           WHEN media_source IN ('zucks_int', 'zucksaffiliate_int', 'smartnewsads_int') THEN campaign
           ELSE af_c_id
        END,
        CONCAT(GAME_NAME, ' ', PLATFORM , ' ', 'None'))
       AS Campaign_Id,

      COALESCE(
        CASE WHEN media_source = 'organic' THEN CONCAT(GAME_NAME, ' ', PLATFORM , ' ', SOURCE)
             WHEN media_source = 'facebook ads' THEN coalesce(fb_adset_id, af_adset_id)
             WHEN media_source = 'googleadwords_int' THEN 'None'
             WHEN agency = 'Mobvista' THEN 'None'
             WHEN media_source = 'beeswax_int' THEN coalesce(af_siteid,af_sub1)
             WHEN media_source = 'twitter' THEN af_adset
             WHEN media_source = 'bytedanceglobal_int' THEN af_adset_id
             WHEN media_source = 'apple search ads' THEN coalesce(af_keywords, af_adset_id)
             WHEN media_source = 'snapchat_int' THEN af_adset_id
             WHEN media_source = 'taboola_int' THEN af_c_id
             WHEN media_source IN ('zucks_int', 'zucksaffiliate_int', 'smartnewsads_int') THEN campaign
             ELSE af_siteid
        END,
        'None') AS Sub_Campaign_Id,

      COALESCE(
          CASE WHEN media_source = 'organic' THEN 'Organic'
               WHEN media_source = 'facebook ads' THEN coalesce(fb_campaign_name, campaign)
               WHEN media_source = 'beeswax_int' THEN af_adset
               WHEN agency = 'Mobvista' THEN campaign
               WHEN media_source = 'apple search ads' THEN af_adset
               WHEN af_channel = 'Kayzen' THEN af_adset
               WHEN media_source IN ('zucks_int', 'zucksaffiliate_int', 'smartnewsads_int') THEN campaign
          ELSE COALESCE(campaign, 'None')
      END,
        Campaign_Id, 'None') AS Campaign_Name,

      COALESCE(
          CASE WHEN media_source = 'organic' THEN 'Organic'
               WHEN media_source = 'facebook ads' THEN coalesce(fb_adset_name, af_adset)
               WHEN media_source = 'googleadwords_int' THEN 'None'
               WHEN media_source = 'beeswax_int' THEN af_sub1
               WHEN media_source = 'snapchat_int' THEN af_adset
               WHEN media_source = 'twitter' THEN rawcampaignname
               WHEN media_source = 'bytedanceglobal_int' THEN af_adset
               WHEN media_source = 'mistplay_int' THEN af_siteid
               WHEN media_source = 'apple search ads' THEN coalesce(af_keywords, af_adset_id)
               WHEN media_source IN ('zucks_int', 'zucksaffiliate_int', 'smartnewsads_int') THEN campaign
          ELSE COALESCE(af_sub1, af_siteid, 'None')
      END,
        Sub_Campaign_Id) AS Sub_Campaign_Name,

     CASE WHEN event_name = 'af_ad_watched' THEN 0
               WHEN event_name = 'af_purchase' THEN 0
               WHEN Media_Source = 'ironsource_int' AND Campaign_Name LIKE '%CPE%' THEN NULL
               WHEN Media_Source = 'tapjoy_int' AND Campaign_Name LIKE '%CPE%' THEN NULL
              ELSE af_cost_value
      END AS usd_reported_cost_per_install,
      COALESCE(
        CASE WHEN media_source = 'facebook ads' THEN 'Facebook'
             WHEN media_source = 'restricted' THEN 'Facebook'
             WHEN media_source = 'googleadwords_int' THEN 'Google'
             WHEN media_source = 'crossinstall_int' THEN 'Crossinstall'
             WHEN media_source = 'applovin_int' THEN 'Applovin'
             WHEN media_source = 'unityads_int' THEN 'Unity'
             WHEN media_source = 'ironsource_int' THEN 'Ironsource'
             WHEN media_source = 'dataliftretargeting_int' AND af_channel <> 'Kayzen' THEN 'Datalift'
             WHEN media_source = 'dataliftretargeting_int' AND af_channel = 'Kayzen' THEN 'Kayzen'
             WHEN media_source = 'getsocial_int' THEN 'Getsocial'
             WHEN media_source = 'tapjoy_int' THEN 'Tapjoy'
             WHEN media_source = 'mintegral_int' THEN 'Mintegral'
             WHEN media_source = 'blast_int' THEN 'Blast'
             WHEN media_source = 'mistplay_int' THEN 'Mistplay'
             WHEN media_source = 'jetfuelit_int' THEN 'Jetfuel'
             WHEN media_source = 'applike_int' THEN 'Applike'
             WHEN media_source = 'beeswax_int' THEN 'Beeswax'
             WHEN agency = 'Mobvista' THEN 'Mobvista'
             WHEN media_source = 'tresensa_int' THEN 'Tresensa'
             WHEN media_source = 'liftoff_int' THEN 'Liftoff'
             WHEN media_source = 'snapchat_int' THEN 'Snapchat'
             WHEN media_source = 'chartboosts2s_int' THEN 'Chartboost'
             WHEN media_source = 'adcolony_int' THEN 'AdColony'
             WHEN media_source = 'bytedanceglobal_int' THEN 'TikTok'
             WHEN media_source = 'vungle_int' THEN 'Vungle'
             WHEN media_source = 'organic' THEN 'Organic'
             WHEN media_source = 'apple search ads' THEN 'Apple Search Ads'
             WHEN media_source = 'sony_xperia' THEN 'Sony_Xperia'
             WHEN media_source = 'twitter' THEN 'Twitter'
             WHEN media_source = 'taboola_int' THEN 'Taboola'
             WHEN media_source IN ('zucks_int', 'zucksaffiliate_int', 'smartnewsads_int') THEN 'Zucks'
             WHEN media_source = 'appreciateappoffers_int' THEN 'Appreciate'
             WHEN media_source = 'dataseat_int' THEN 'Dataseat'
        ELSE media_source
        END,
        'None')
      AS Media_Source,

      COALESCE(IFF(FLAT.rawcountrycode = 'UK', 'GB', FLAT.rawcountrycode), 'None') AS COUNTRY_CODE,
      DATEDIFF('days',TO_DATE(install_time),TO_DATE(event_time)) days_since_install,
      IFNULL(for_testing, FALSE) for_testing_filter,
      ROW_NUMBER() OVER (PARTITION BY event_type, event_time, appsflyer_device_id ORDER BY event_time DESC) KY_Unique,
      SERVER_TIME,
      _Tst,
     CASE  WHEN language = 'español' THEN 'spanish'
             WHEN language = 'русский' THEN 'russian'
             WHEN language = 'ru-ru' THEN 'russian'
             WHEN language = 'português' THEN 'portugese'
             WHEN language = 'en-us' THEN 'english'
             WHEN language = 'en-gb' THEN 'english'
             WHEN language = 'en-au' THEN 'english'
             WHEN language = 'en-ca' THEN 'english'
             WHEN language = 'türkçe' THEN 'turkish'
             WHEN language = 'tr-tr' THEN 'turkish'
             WHEN language = 'العربية' THEN 'arabic'
             WHEN language = 'français' THEN 'french'
             WHEN language = 'polski' THEN 'polish'
             WHEN language = 'zh-hans-cn' THEN 'chinese'
             WHEN language = 'tiếng việt' THEN 'vietnamese'
             WHEN language = 'indonesia' THEN 'indonesian'
             WHEN language = '한국어' THEN 'korean'
             WHEN language = 'ไทย' THEN 'thai'
             WHEN language = 'italiano' THEN 'italian'
             WHEN language = 'de-de' THEN 'german'
             WHEN language = 'deutsch' THEN 'german'
             WHEN language = 'fr-fr' THEN 'french'
             WHEN language = 'ja-jp' THEN 'japanese'
             WHEN language = 'čeština' THEN 'czech'
             WHEN language = 'nl-nl' THEN 'dutch'
             WHEN language = 'nederlands' THEN 'dutch'
        ELSE language
        END
      AS language
  FROM FLAT
  WHERE
      for_testing_filter = FALSE
      AND COUNTRY_CODE IS NOT NULL
      AND days_since_install >= 0
      AND TO_DATE(install_time) >= '2016-09-01'
      AND event_time >= install_time
      AND Media_Source NOT ILIKE '%test%'
      AND GAME_NAME IS NOT NULL
      AND (COUNTRY_CODE <> 'CN' OR PLATFORM <> 'Android')
      AND IS_RETARGETING = {IS_RETARGETING}
      AND {SLICE}
      {FILTER_QUERY}
) MAIN
----Back-up join to remove country codes that don't match DIM_COUNTRY
LEFT JOIN L1_REFERENCE_DATA.DIM_COUNTRY COUNTRY
ON COUNTRY.COUNTRY_CODE = MAIN.COUNTRY_CODE

--cheaters
LEFT JOIN L2_BLACKLIST_PLAYERS.PURCHASE_CHEATER CHEATER
ON MAIN.customer_user_id = CHEATER.CUSTOMER_USER_ID

---Removing qa blacklist devices
LEFT JOIN L2_BLACKLIST_PLAYERS.CLIENTSDK_QA_DEVICE_LIST BLACKLIST
ON MAIN.customer_user_id = BLACKLIST.DEVICE_ID

WHERE KY_Unique = 1
    AND COUNTRY.COUNTRY_CODE IS NOT NULL
    AND CHEATER.CUSTOMER_USER_ID IS NULL
    AND BLACKLIST.DEVICE_ID IS NULL
;

COMMIT;
