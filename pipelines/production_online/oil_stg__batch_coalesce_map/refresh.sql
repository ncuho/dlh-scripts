-- Связывает исходные партии с объединённой партией аудита. В новом выпуске используются сохранённый INITIAL и raw-окно C0–C1.
SELECT original_batch_id,
       coalesced_batch_id,
       source_name,
       original_event_count,
       created_at
FROM {{initial:oil_stg__batch_coalesce_map}};
