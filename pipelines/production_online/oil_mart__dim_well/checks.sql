-- Контекст миграции: AS IS / TO BE.
-- Назначение: Формирует SCD2-версии скважин и принадлежность месторождению.
-- AS IS: oil_mart.dim_well в GP; выбранная исходная бизнес-сущность.
-- TO BE: Cedrus исполняет только проверки текущей phase и both; публикация разрешена при нулевых нарушениях.
-- Отличие: PK/FK/UNIQUE Iceberg не обеспечивает; SQL проверяет grain, дубли, ссылки, SCD2 и равенство на C0/C1.
-- Конец контекста миграции.
-- Проверка: initial_full_equality; phase=both
-- INITIAL: полное равенство истории скважин и интервалов [valid_from,valid_to).
-- Единственный результат n обязан быть 0; иначе публикация блокируется.
WITH s AS
    (SELECT well_hk,
            field_hk,
            entity_version,
            valid_from,
            valid_to,
            well_name,
            status,
            is_deleted
     FROM {{source:oil_mart__dim_well}}),
     t AS
    (SELECT well_hk,
            field_hk,
            entity_version,
            valid_from,
            valid_to,
            well_name,
            status,
            is_deleted
     FROM {{target:oil_mart__dim_well}}),
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

-- Проверка: ambiguous_effective_dates; phase=both
-- INITIAL/REFRESH: исходный запрет неоднозначной даты начала версии скважины.
-- Единственный результат n обязан быть 0; иначе публикация блокируется.
SELECT count(*) AS n
FROM
    (SELECT well_hk,
            valid_from
     FROM {{target:oil_dds__sat_well}}
     GROUP BY well_hk,
              valid_from
     HAVING count(*)>1) q;

-- Проверка: scd2_rebuild_equality; phase=both
-- INITIAL/REFRESH: полное равенство пересчёту из satellite и link текущего релиза.
-- Единственный результат n обязан быть 0; иначе публикация блокируется.
WITH e AS
    (SELECT w.well_hk,
            l.field_hk,
            w.entity_version,
            w.valid_from,
            lead(w.valid_from) OVER (PARTITION BY w.well_hk
                                     ORDER BY w.valid_from, w.entity_version) AS valid_to,
            w.well_name,
            w.status,
            w.op='D' AS is_deleted
     FROM {{target:oil_dds__sat_well}} w
     JOIN {{target:oil_dds__link_well_field}} l ON l.link_hk=w.link_hk),
     t AS
    (SELECT well_hk,
            field_hk,
            entity_version,
            valid_from,
            valid_to,
            well_name,
            status,
            is_deleted
     FROM {{target:oil_mart__dim_well}}),
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
