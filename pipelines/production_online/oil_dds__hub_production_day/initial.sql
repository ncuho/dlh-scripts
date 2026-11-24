-- Копирует объект согласованного среза GP без пересчёта бизнес-логики; запись выполняет CedrusData Engine.
SELECT production_hk,production_id,load_dts,record_source FROM {{source:oil_dds__hub_production_day}};
