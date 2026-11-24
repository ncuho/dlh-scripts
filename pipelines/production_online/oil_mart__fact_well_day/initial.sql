-- Копирует объект согласованного среза GP без пересчёта бизнес-логики; запись выполняет CedrusData Engine.
SELECT production_hk,well_hk,field_hk,production_date,well_version,field_version,status,oil_tonnes FROM {{source:oil_mart__fact_well_day}};
