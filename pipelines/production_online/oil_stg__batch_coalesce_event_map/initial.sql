-- Сохраняет происхождение доставки после объединения партий. История читается из C0; исходная GP-функция повторно не запускается.
SELECT topic,
       kafka_partition,
       kafka_offset,
       original_batch_id,
       coalesced_batch_id
FROM {{source:oil_stg__batch_coalesce_event_map}};
