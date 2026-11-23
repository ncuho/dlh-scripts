-- Ранжируем доставки event_id; точный replay не создаёт второе событие, исходный INITIAL-аудит сохраняется. Это целевой SQL-этап вместо части apply_batch GP.
SELECT event_id,
       event_type,
       entity_id,
       entity_version,
       op,
       event_time,
       load_dts,
       record_source,
       batch_id,
       payload,
       event_document
FROM {{source:oil_dds__event}};
