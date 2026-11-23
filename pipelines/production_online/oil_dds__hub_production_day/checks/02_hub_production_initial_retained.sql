-- Сохранение исходных ключей и аудита при REFRESH.
SELECT count(*) AS n FROM (SELECT production_hk,production_id,load_dts,record_source FROM {{initial:oil_dds__hub_production_day}} EXCEPT ALL SELECT production_hk,production_id,load_dts,record_source FROM {{target:oil_dds__hub_production_day}}) q;
