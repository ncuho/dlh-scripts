-- Контекст миграции: AS IS / TO BE.
-- Назначение: Формирует SCD2-версии месторождений с интервалом действия.
-- AS IS: oil_mart.dim_field в GP; выбранная исходная бизнес-сущность.
-- TO BE: Cedrus исполняет только проверки текущей phase и both; публикация разрешена при нулевых нарушениях.
-- Отличие: PK/FK/UNIQUE Iceberg не обеспечивает; SQL проверяет grain, дубли, ссылки, SCD2 и равенство на C0/C1.
-- Конец контекста миграции.
-- Проверка: initial_full_equality; phase=both
-- INITIAL: полное равенство SCD2, включая конечные границы и удаления.
-- Единственный результат n обязан быть 0; иначе публикация блокируется.
WITH s AS
    (SELECT field_hk,
            entity_version,
            valid_from,
            valid_to,
            field_name,
            region,
            is_deleted
     FROM {{source:oil_mart__dim_field}}),
     t AS
    (SELECT field_hk,
            entity_version,
            valid_from,
            valid_to,
            field_name,
            region,
            is_deleted
     FROM {{target:oil_mart__dim_field}}),
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
-- INITIAL/REFRESH: исходный запрет неоднозначной даты начала версии месторождения.
-- Единственный результат n обязан быть 0; иначе публикация блокируется.
SELECT count(*) AS n
FROM
    (SELECT field_hk,
            event_time
     FROM {{target:oil_dds__sat_field}}
     GROUP BY field_hk,
              event_time
     HAVING count(*)>1) q;

-- Проверка: scd2_rebuild_equality; phase=both
-- INITIAL/REFRESH: полное равенство измерения пересчёту из текущей истории DDS.
-- Единственный результат n обязан быть 0; иначе публикация блокируется.
WITH e AS
    (SELECT field_hk,
            entity_version,
            event_time AS valid_from,
            lead(event_time) OVER (PARTITION BY field_hk
                                   ORDER BY event_time, entity_version) AS valid_to,
            field_name,
            region,
            op='D' AS is_deleted
     FROM {{target:oil_dds__sat_field}}),
     t AS
    (SELECT field_hk,
            entity_version,
            valid_from,
            valid_to,
            field_name,
            region,
            is_deleted
     FROM {{target:oil_mart__dim_field}}),
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
