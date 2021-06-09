DROP MATERIALIZED VIEW IF EXISTS public.clientsdk_aggregated_user_view;

CREATE MATERIALIZED VIEW public.clientsdk_aggregated_user_view AS
  SELECT * FROM ( SELECT *, row_number() over (partition by playfab_id order by _tst desc) as rn from public.clientsdk_aggregated_user_landing) t where rn = 1;

CREATE UNIQUE INDEX uid ON public.clientsdk_aggregated_user_view (playfab_id);
