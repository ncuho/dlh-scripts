-- LEAD(event_time) строит SCD2-интервалы месторождений; как в refresh_production GP, версия D сохраняется как is_deleted. Расчёт выполняется вместо соответствующей части refresh_production GP.
SELECT field_hk,
       entity_version,
       event_time AS valid_from,
       lead(event_time) OVER (PARTITION BY field_hk
                              ORDER BY event_time, entity_version) AS valid_to,
       field_name,
       region,
       op='D' AS is_deleted
FROM {{target:oil_dds__sat_field}};
