-- Проверка: initial_full_equality; phase=both
-- INITIAL: полное равенство факта, выбранных версий измерений, статуса и добычи.
-- Единственный результат n обязан быть 0; иначе публикация блокируется.
WITH s AS
    (SELECT production_hk,
            well_hk,
            field_hk,
            production_date,
            well_version,
            field_version,
            status,
            oil_tonnes
     FROM {{source:oil_mart__fact_well_day}}),
     t AS
    (SELECT production_hk,
            well_hk,
            field_hk,
            production_date,
            well_version,
            field_version,
            status,
            oil_tonnes
     FROM {{target:oil_mart__fact_well_day}}),
     d AS (
               (SELECT *
                FROM s
                EXCEPT ALL SELECT *
                FROM t)
           UNION ALL
               (SELECT *
                FROM t
                EXCEPT ALL SELECT *
                FROM s))
SELECT count(*) AS n
FROM d;

-- Проверка: production_dimension_cardinality; phase=both
-- INITIAL/REFRESH: каждой последней U-версии соответствует ровно одна строка факта.
-- Единственный результат n обязан быть 0; иначе публикация блокируется.
WITH p AS
    (SELECT production_hk,
            op,
            row_number() OVER (PARTITION BY production_hk
                               ORDER BY entity_version DESC, event_id DESC) AS rn
     FROM {{target:oil_dds__sat_daily_production}})
SELECT count(*) AS n
FROM p
WHERE rn=1
    AND op='U'
    AND
        (SELECT count(*)
         FROM {{target:oil_mart__fact_well_day}} f
         WHERE f.production_hk=p.production_hk)<>1;

-- Проверка: well_status_consistency; phase=both
-- INITIAL/REFRESH: исходная проверка согласованности статуса и нулевой добычи INACTIVE.
-- Единственный результат n обязан быть 0; иначе публикация блокируется.
SELECT count(*) AS n
FROM {{target:oil_mart__fact_well_day}} p
JOIN {{target:oil_mart__dim_well}} w ON p.well_hk=w.well_hk
AND p.well_version=w.entity_version
WHERE p.status<>w.status
    OR (p.status='INACTIVE'
        AND p.oil_tonnes<>0);

-- Проверка: fact_rebuild_equality; phase=both
-- INITIAL/REFRESH: полное равенство повторному построению; выявляет лишние факты, удаления и неверный выбор SCD2.
-- Единственный результат n обязан быть 0; иначе публикация блокируется.
WITH p AS
    (SELECT production_hk,
            link_hk,
            production_date,
            status,
            oil_tonnes,
            op,
            row_number() OVER (PARTITION BY production_hk
                               ORDER BY entity_version DESC, event_id DESC) AS rn
     FROM {{target:oil_dds__sat_daily_production}}),
     e AS
    (SELECT p.production_hk,
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
     AND f.valid_from<=with_timezone(CAST(p.production_date AS timestamp), 'UTC')
     AND (f.valid_to IS NULL
          OR with_timezone(CAST(p.production_date AS timestamp), 'UTC')<f.valid_to)
     WHERE p.rn=1
         AND p.op='U'),
     t AS
    (SELECT production_hk,
            well_hk,
            field_hk,
            production_date,
            well_version,
            field_version,
            status,
            oil_tonnes
     FROM {{target:oil_mart__fact_well_day}}),
     d AS (
               (SELECT *
                FROM e
                EXCEPT ALL SELECT *
                FROM t)
           UNION ALL
               (SELECT *
                FROM t
                EXCEPT ALL SELECT *
                FROM e))
SELECT count(*) AS n
FROM d;
