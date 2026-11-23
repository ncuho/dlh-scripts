-- Контекст миграции: AS IS / TO BE.
-- Назначение: Суммирует тонны и активные скважино-дни по месторождению и дате.
-- AS IS: oil_mart.agg_field_day в GP; выбранная исходная бизнес-сущность.
-- TO BE: Cedrus исполняет только проверки текущей phase и both; публикация разрешена при нулевых нарушениях.
-- Отличие: PK/FK/UNIQUE Iceberg не обеспечивает; SQL проверяет grain, дубли, ссылки, SCD2 и равенство на C0/C1.
-- Конец контекста миграции.
-- Проверка: initial_full_equality; phase=both
-- INITIAL: полное равенство зерна и всех показателей, включая NULL при нулевом знаменателе.
-- Единственный результат n обязан быть 0; иначе публикация блокируется.
WITH s AS
    (SELECT production_date,
            field_hk,
            field_name,
            region,
            oil_tonnes,
            producing_well_days,
            oil_per_well_day
     FROM {{source:oil_mart__agg_field_day}}),
     t AS
    (SELECT production_date,
            field_hk,
            field_name,
            region,
            oil_tonnes,
            producing_well_days,
            oil_per_well_day
     FROM {{target:oil_mart__agg_field_day}}),
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

-- Проверка: aggregate_components; phase=both
-- INITIAL/REFRESH: полное равенство групп, суммы добычи и числа ACTIVE-строк пересчёту из факта.
-- Единственный результат n обязан быть 0; иначе публикация блокируется.
WITH e AS
    (SELECT p.production_date,
            p.field_hk,
            f.field_name,
            f.region,
            CAST(sum(p.oil_tonnes) AS decimal(22, 3)) AS oil_tonnes,
            sum(CASE
                    WHEN p.status='ACTIVE' THEN CAST(1 AS bigint)
                    ELSE CAST(0 AS bigint)
                END) AS producing_well_days
     FROM {{target:oil_mart__fact_well_day}} p
     JOIN {{target:oil_mart__dim_field}} f ON f.field_hk=p.field_hk
     AND f.entity_version=p.field_version
     GROUP BY p.production_date,
              p.field_hk,
              f.field_name,
              f.region),
     t AS
    (SELECT production_date,
            field_hk,
            field_name,
            region,
            oil_tonnes,
            producing_well_days
     FROM {{target:oil_mart__agg_field_day}}),
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

-- Проверка: ratio_exact_rounding; phase=both
-- INITIAL/REFRESH: проверка округления отношения до 6 знаков через целые частное и остаток для неотрицательной добычи.
-- Единственный результат n обязан быть 0; иначе публикация блокируется.
WITH a AS
    (SELECT oil_tonnes,
            producing_well_days,
            oil_per_well_day,
            CAST(oil_tonnes*DECIMAL '1000000' AS decimal(25, 0)) AS numerator
     FROM {{target:oil_mart__agg_field_day}}),
     r AS
    (SELECT *,
            mod(numerator, CAST(nullif(producing_well_days, 0) AS decimal(19, 0))) AS remainder
     FROM a),
     e AS
    (SELECT *,
            CAST((CAST((numerator-remainder)/CAST(nullif(producing_well_days, 0) AS decimal(19, 0)) AS decimal(25, 0))+CASE
                                                                                                                           WHEN remainder*DECIMAL '2'>=producing_well_days THEN DECIMAL '1'
                                                                                                                           ELSE DECIMAL '0'
                                                                                                                       END)*DECIMAL '0.000001' AS decimal(22, 6)) AS expected
     FROM r)
SELECT count(*) AS n
FROM e
WHERE oil_tonnes<0
    OR producing_well_days<0
    OR oil_per_well_day IS DISTINCT
    FROM expected;
