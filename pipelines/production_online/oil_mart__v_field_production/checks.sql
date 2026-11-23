-- Полное равенство serving-результата исходному представлению.
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


-- Serving полностью совпадает с агрегатом текущего релиза.
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
