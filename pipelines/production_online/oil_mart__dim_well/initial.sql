-- Копирует объект согласованного среза GP без пересчёта бизнес-логики; запись выполняет CedrusData Engine.
SELECT well_hk,field_hk,entity_version,valid_from,valid_to,well_name,status,is_deleted FROM {{source:oil_mart__dim_well}};
