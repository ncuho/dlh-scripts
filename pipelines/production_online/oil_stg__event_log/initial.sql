-- Копирует объект согласованного среза GP без пересчёта бизнес-логики; запись выполняет CedrusData Engine.
SELECT topic,kafka_partition,kafka_offset,batch_id,received_at,event_json FROM {{source:oil_stg__event_log}};
