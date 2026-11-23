-- Суммирует тонны и активные скважино-дни по месторождению и дате.
CREATE TABLE dlh.gold.agg_field_day (
    "production_date" date,
    "field_hk" varchar,
    "field_name" varchar,
    "region" varchar,
    "oil_tonnes" decimal(22,3),
    "producing_well_days" bigint,
    "oil_per_well_day" decimal(22,6)
) WITH (format='PARQUET', format_version=2);
