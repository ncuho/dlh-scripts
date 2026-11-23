-- Из field-событий строим field hash key и версии атрибутов; исходный payload определяет hashdiff. Это целевой SQL-этап вместо части apply_batch GP.
SELECT field_hk,
       event_id,
       entity_version,
       op,
       event_time,
       load_dts,
       record_source,
       hashdiff,
       field_name,
       region
FROM {{source:oil_dds__sat_field}};
