-- Сохранить статусы, manifests и остальные исходные audit-поля.
WITH s AS
    (SELECT batch_id,
            created_at,
            error_text,
            source_manifest,
            source_name,
            status
     FROM {{source:oil_stg__batch_control}}),
     t AS
    (SELECT batch_id,
            created_at,
            error_text,
            source_manifest,
            source_name,
            status
     FROM {{target:oil_stg__batch_control}}),
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
