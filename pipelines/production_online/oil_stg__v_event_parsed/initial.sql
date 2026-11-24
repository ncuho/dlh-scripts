-- Копирует объект согласованного среза GP без пересчёта бизнес-логики; запись выполняет CedrusData Engine.
SELECT topic,kafka_partition,kafka_offset,batch_id,event_json,schema_version,event_id,event_type,entity_id,entity_version,op,event_time,payload FROM {{source:oil_stg__v_event_parsed}};
