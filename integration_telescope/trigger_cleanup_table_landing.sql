delete from public.clientsdk_aggregated_user_landing where _tst < (now() - INTERVAL '7 DAYS')
