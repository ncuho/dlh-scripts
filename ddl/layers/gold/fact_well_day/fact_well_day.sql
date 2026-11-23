-- Связывает суточную добычу с действовавшими версиями измерений.
CREATE TABLE dlh.gold.fact_well_day (
    "production_hk" varchar,
    "well_hk" varchar,
    "field_hk" varchar,
    "production_date" date,
    "well_version" bigint,
    "field_version" bigint,
    "status" varchar,
    "oil_tonnes" decimal(18,3)
) WITH (format='PARQUET', format_version=2);
