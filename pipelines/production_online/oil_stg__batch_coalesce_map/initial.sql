-- Копирует объект согласованного среза GP без пересчёта бизнес-логики; запись выполняет CedrusData Engine.
SELECT original_batch_id,coalesced_batch_id,source_name,original_event_count,created_at FROM {{source:oil_stg__batch_coalesce_map}};
