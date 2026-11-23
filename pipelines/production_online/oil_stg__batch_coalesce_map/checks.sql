-- Сохранить все поля исходного аудита объединения партий.
WITH s AS
    (SELECT original_batch_id,
            coalesced_batch_id,
            source_name,
            original_event_count,
            created_at
     FROM {{source:oil_stg__batch_coalesce_map}}),
     t AS
    (SELECT original_batch_id,
            coalesced_batch_id,
            source_name,
            original_event_count,
            created_at
     FROM {{target:oil_stg__batch_coalesce_map}}),
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
