-- Из production_day строим ключ скважино-дня, связь со скважиной и версионированные Decimal-тонны/статус. Это целевой SQL-этап вместо части apply_batch GP.
SELECT production_hk,
       link_hk,
       event_id,
       entity_version,
       op,
       event_time,
       load_dts,
       record_source,
       hashdiff,
       production_date,
       oil_tonnes,
       status
FROM {{source:oil_dds__sat_daily_production}};
