-- Копирует объект согласованного среза GP без пересчёта бизнес-логики; запись выполняет CedrusData Engine.
SELECT link_hk,well_hk,field_hk,load_dts,record_source FROM {{source:oil_dds__link_well_field}};
