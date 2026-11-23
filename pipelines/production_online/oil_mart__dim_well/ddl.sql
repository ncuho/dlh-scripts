-- Cedrus/Trino. Малый immutable release: Parquet v2, без искусственных PK/FK.
CREATE TABLE {{target:oil_mart__dim_well}} (
    "well_hk" varchar,
    "field_hk" varchar,
    "entity_version" bigint,
    "valid_from" date,
    "valid_to" date,
    "well_name" varchar,
    "status" varchar,
    "is_deleted" boolean
) WITH (format='PARQUET', format_version=2);
