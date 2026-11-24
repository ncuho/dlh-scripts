-- Копирует объект согласованного среза GP без пересчёта бизнес-логики; запись выполняет CedrusData Engine.
SELECT field_hk,event_id,entity_version,op,event_time,load_dts,record_source,hashdiff,field_name,region FROM {{source:oil_dds__sat_field}};
