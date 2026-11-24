-- Копирует объект согласованного среза GP без пересчёта бизнес-логики; запись выполняет CedrusData Engine.
SELECT topic,kafka_partition,kafka_offset,original_batch_id,coalesced_batch_id FROM {{source:oil_stg__batch_coalesce_event_map}};
