-- Сохраняет происхождение доставки после объединения партий. Здесь сохраняется аудит GP на C0, а не checkpoint нового sink.
SELECT topic,
       kafka_partition,
       kafka_offset,
       original_batch_id,
       coalesced_batch_id
FROM {{initial:oil_stg__batch_coalesce_event_map}};
