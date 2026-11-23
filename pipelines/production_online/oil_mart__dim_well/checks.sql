-- Полное равенство истории скважин и интервалов [valid_from,valid_to).
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


-- Исходный запрет неоднозначной даты начала версии скважины.
SELECT count(*) AS n
FROM
    (SELECT well_hk,
            valid_from
     FROM {{target:oil_dds__sat_well}}
     GROUP BY well_hk,
              valid_from
     HAVING count(*)>1) q;


-- Полное равенство пересчёту из satellite и link текущего релиза.
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
