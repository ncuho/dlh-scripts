-- Копирует объект согласованного среза GP без пересчёта бизнес-логики; запись выполняет CedrusData Engine.
SELECT production_date,field_hk,field_name,region,oil_tonnes,producing_well_days,oil_per_well_day FROM {{source:oil_mart__agg_field_day}};
