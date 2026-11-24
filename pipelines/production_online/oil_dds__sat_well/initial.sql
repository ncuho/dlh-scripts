-- Копирует объект согласованного среза GP без пересчёта бизнес-логики; запись выполняет CedrusData Engine.
SELECT well_hk,link_hk,event_id,entity_version,op,event_time,load_dts,record_source,hashdiff,well_name,valid_from,status FROM {{source:oil_dds__sat_well}};
