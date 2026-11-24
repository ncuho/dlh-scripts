-- Обновляет существующий объект из предыдущего принятого snapshot и ограниченного окна raw; _next не создаётся.
-- Row_number выбирает последнюю production-версию; U/D и исторические интервалы dim_well/dim_field определяют единственную строку факта. Расчёт выполняется вместо соответствующей части refresh_production GP.
WITH p AS
    (SELECT production_hk,
            link_hk,
            production_date,
            status,
            oil_tonnes,
            op,
            row_number() OVER (PARTITION BY production_hk
                               ORDER BY entity_version DESC, event_id DESC) AS rn
     FROM {{target:oil_dds__sat_daily_production}})
SELECT p.production_hk,
       l.well_hk,
       w.field_hk,
       p.production_date,
       w.entity_version AS well_version,
       f.entity_version AS field_version,
       p.status,
       p.oil_tonnes
FROM p
JOIN {{target:oil_dds__link_production_day_well}} l ON l.link_hk=p.link_hk
JOIN {{target:oil_mart__dim_well}} w ON w.well_hk=l.well_hk
AND NOT w.is_deleted
AND w.valid_from<=p.production_date
AND (w.valid_to IS NULL
     OR p.production_date<w.valid_to)
JOIN {{target:oil_mart__dim_field}} f ON f.field_hk=w.field_hk
AND NOT f.is_deleted
AND f.valid_from<=to_timestamp(concat(CAST(p.production_date AS STRING), 'T00:00:00+00:00'))
AND (f.valid_to IS NULL
     OR to_timestamp(concat(CAST(p.production_date AS STRING), 'T00:00:00+00:00'))<f.valid_to)
WHERE p.rn=1
    AND p.op='U';
