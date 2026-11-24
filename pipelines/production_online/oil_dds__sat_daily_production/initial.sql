-- Копирует объект согласованного среза GP без пересчёта бизнес-логики; запись выполняет CedrusData Engine.
SELECT production_hk,link_hk,event_id,entity_version,op,event_time,load_dts,record_source,hashdiff,production_date,oil_tonnes,status FROM {{source:oil_dds__sat_daily_production}};
