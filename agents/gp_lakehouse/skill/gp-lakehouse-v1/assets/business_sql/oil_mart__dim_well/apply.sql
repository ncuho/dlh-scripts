-- Обновляет существующий объект из предыдущего принятого snapshot и ограниченного окна raw; _next не создаётся.
-- LEAD(valid_from) закрывает SCD2-интервалы скважины, link_well_field даёт историческое месторождение. Расчёт выполняется вместо соответствующей части refresh_production GP.
SELECT w.well_hk,
       l.field_hk,
       w.entity_version,
       w.valid_from,
       lead(w.valid_from) OVER (PARTITION BY w.well_hk
                                ORDER BY w.valid_from, w.entity_version) AS valid_to,
       w.well_name,
       w.status,
       w.op='D' AS is_deleted
FROM {{target:oil_dds__sat_well}} w
JOIN {{target:oil_dds__link_well_field}} l ON l.link_hk=w.link_hk;
