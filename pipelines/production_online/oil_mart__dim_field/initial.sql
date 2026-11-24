-- Копирует объект согласованного среза GP без пересчёта бизнес-логики; запись выполняет CedrusData Engine.
SELECT field_hk,entity_version,valid_from,valid_to,field_name,region,is_deleted FROM {{source:oil_mart__dim_field}};
