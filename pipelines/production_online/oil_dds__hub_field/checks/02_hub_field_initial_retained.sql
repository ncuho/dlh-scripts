-- Существующие ключи сохраняют исходные audit-поля.
SELECT count(*) AS n FROM (SELECT field_hk,field_id,load_dts,record_source FROM {{initial:oil_dds__hub_field}} EXCEPT ALL SELECT field_hk,field_id,load_dts,record_source FROM {{target:oil_dds__hub_field}}) q;
