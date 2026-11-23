-- Связывает исходные партии с объединённой партией аудита. История читается из C0; исходная GP-функция повторно не запускается.
SELECT original_batch_id,
       coalesced_batch_id,
       source_name,
       original_event_count,
       created_at
FROM {{source:oil_stg__batch_coalesce_map}};
