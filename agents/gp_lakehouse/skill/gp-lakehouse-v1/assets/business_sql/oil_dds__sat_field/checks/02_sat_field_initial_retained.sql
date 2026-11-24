-- Все исходные версии и их аудит сохранены без изменений.
SELECT count(*) AS n FROM (SELECT field_hk,event_id,entity_version,op,event_time,load_dts,record_source,hashdiff,field_name,region FROM {{previous:oil_dds__sat_field}} EXCEPT ALL SELECT field_hk,event_id,entity_version,op,event_time,load_dts,record_source,hashdiff,field_name,region FROM {{target:oil_dds__sat_field}}) q;
