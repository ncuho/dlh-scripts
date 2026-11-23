-- Неизменность первоначальных ключей и аудита.
SELECT count(*) AS n FROM (SELECT well_hk,well_id,load_dts,record_source FROM {{initial:oil_dds__hub_well}} EXCEPT ALL SELECT well_hk,well_id,load_dts,record_source FROM {{target:oil_dds__hub_well}}) q;
