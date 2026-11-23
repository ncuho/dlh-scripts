-- Полное равенство SCD2, включая конечные границы и удаления.
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


-- Исходный запрет неоднозначной даты начала версии месторождения.
SELECT count(*) AS n
FROM
    (SELECT field_hk,
            event_time
     FROM {{target:oil_dds__sat_field}}
     GROUP BY field_hk,
              event_time
     HAVING count(*)>1) q;


-- Полное равенство измерения пересчёту из текущей истории DDS.
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
