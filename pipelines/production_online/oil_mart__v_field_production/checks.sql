-- Контекст миграции: AS IS / TO BE.
-- Назначение: Предоставляет бизнес-колонки отчёта добычи по месторождениям.
-- AS IS: oil_mart.v_field_production в GP; выбранная исходная бизнес-сущность.
-- TO BE: Cedrus исполняет только проверки текущей phase и both; публикация разрешена при нулевых нарушениях.
-- Отличие: PK/FK/UNIQUE Iceberg не обеспечивает; SQL проверяет grain, дубли, ссылки, SCD2 и равенство на C0/C1.
-- Конец контекста миграции.
-- Проверка: initial_full_equality; phase=both
-- INITIAL: полное равенство serving-результата исходному представлению.
-- Единственный результат n обязан быть 0; иначе публикация блокируется.
WITH s AS
    (SELECT production_date,
            field_hk,
            field_name,
            region,
            oil_tonnes,
            producing_well_days,
            oil_per_well_day
     FROM {{source:oil_mart__v_field_production}}),
     t AS
    (SELECT production_date,
            field_hk,
            field_name,
            region,
            oil_tonnes,
            producing_well_days,
            oil_per_well_day
     FROM {{target:oil_mart__v_field_production}}),
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

-- Проверка: serving_rebuild_equality; phase=both
-- INITIAL/REFRESH: serving полностью совпадает с агрегатом текущего релиза.
-- Единственный результат n обязан быть 0; иначе публикация блокируется.
WITH e AS
    (SELECT production_date,
            field_hk,
            field_name,
            region,
            oil_tonnes,
            producing_well_days,
            oil_per_well_day
     FROM {{target:oil_mart__agg_field_day}}),
     t AS
    (SELECT production_date,
            field_hk,
            field_name,
            region,
            oil_tonnes,
            producing_well_days,
            oil_per_well_day
     FROM {{target:oil_mart__v_field_production}}),
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
