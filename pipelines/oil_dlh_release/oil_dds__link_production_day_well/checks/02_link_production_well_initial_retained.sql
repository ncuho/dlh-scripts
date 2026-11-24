-- Существующие связи и их исходный аудит не изменены.
SELECT count(*) AS n FROM (SELECT link_hk,production_hk,well_hk,load_dts,record_source FROM {{previous:oil_dds__link_production_day_well}} EXCEPT ALL SELECT link_hk,production_hk,well_hk,load_dts,record_source FROM {{target:oil_dds__link_production_day_well}}) q;
