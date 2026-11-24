-- Копирует объект согласованного среза GP без пересчёта бизнес-логики; запись выполняет CedrusData Engine.
SELECT event_id,event_type,entity_id,entity_version,op,event_time,load_dts,record_source,batch_id,payload,event_document FROM {{source:oil_dds__event}};
