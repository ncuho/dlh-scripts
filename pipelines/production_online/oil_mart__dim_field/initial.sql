-- Историческое измерение месторождения. Интервал действия версии — [valid_from, valid_to); удаление сохраняется отдельным признаком.
-- Engine: spark. Столбцы INSERT берутся в порядке DDL, не по позиции исходного SELECT.
-- INITIAL: согласованный исходный образ C0; исторический аудит не пересоздаётся.
SELECT field_hk,
       entity_version,
       event_time AS valid_from,
       lead(event_time) OVER (PARTITION BY field_hk
                              ORDER BY event_time, entity_version) AS valid_to,
       field_name,
       region,
       op='D' AS is_deleted
FROM {{target:oil_dds__sat_field}};
