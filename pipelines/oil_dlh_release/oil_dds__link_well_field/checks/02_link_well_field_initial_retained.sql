-- Исходные отношения и аудит сохранены.
SELECT count(*) AS n FROM (SELECT link_hk,well_hk,field_hk,load_dts,record_source FROM {{previous:oil_dds__link_well_field}} EXCEPT ALL SELECT link_hk,well_hk,field_hk,load_dts,record_source FROM {{target:oil_dds__link_well_field}}) q;
