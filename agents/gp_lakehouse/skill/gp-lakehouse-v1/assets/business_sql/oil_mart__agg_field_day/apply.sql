-- Обновляет существующий объект из предыдущего принятого snapshot и ограниченного окна raw; _next не создаётся.
-- Суммируем добычу и активные скважино-дни по месторождению/дате; делим суммы с защитой от нулевого знаменателя. Расчёт выполняется вместо соответствующей части refresh_production GP.
WITH g AS
    (SELECT p.production_date,
            p.field_hk,
            f.field_name,
            f.region,
            CAST(sum(p.oil_tonnes) AS DECIMAL(22, 3)) AS oil_tonnes,
            CAST(sum(CASE
                         WHEN p.status='ACTIVE' THEN 1
                         ELSE 0
                     END) AS BIGINT) AS producing_well_days
     FROM {{target:oil_mart__fact_well_day}} p
     JOIN {{target:oil_mart__dim_field}} f ON f.field_hk=p.field_hk
     AND f.entity_version=p.field_version
     GROUP BY p.production_date,
              p.field_hk,
              f.field_name,
              f.region),
     a AS
    (SELECT *,
            CAST(oil_tonnes*CAST(1000000 AS DECIMAL(7, 0)) AS DECIMAL(25, 0)) AS numerator
     FROM g),
     r AS
    (SELECT *,
            pmod(numerator, CAST(nullif(producing_well_days, 0) AS DECIMAL(19, 0))) AS remainder
     FROM a)
SELECT production_date,
       field_hk,
       field_name,
       region,
       oil_tonnes,
       producing_well_days,
       CAST((CAST((numerator-remainder)/CAST(nullif(producing_well_days, 0) AS DECIMAL(19, 0)) AS DECIMAL(25, 0))+CASE
                                                                                                                      WHEN remainder*CAST(2 AS DECIMAL(1, 0))>=producing_well_days THEN CAST(1 AS DECIMAL(1, 0))
                                                                                                                      ELSE CAST(0 AS DECIMAL(1, 0))
                                                                                                                  END)*CAST(0.000001 AS DECIMAL(7, 6)) AS DECIMAL(22, 6)) AS oil_per_well_day
FROM r;
