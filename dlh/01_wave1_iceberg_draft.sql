-- DRAFT ONLY: validate Cedrus/Trino Iceberg versions, permissions and types before use.

-- Schema/catalog creation and roles are separate. Wave 1 only; do not run migration.

CREATE TABLE dlh.oil_dds.hub_field (field_hk varchar,field_id varchar,load_dts timestamp(6) with time zone,record_source varchar) WITH (format = 'PARQUET');

CREATE TABLE dlh.oil_dds.hub_well (well_hk varchar,well_id varchar,load_dts timestamp(6) with time zone,record_source varchar) WITH (format = 'PARQUET');

CREATE TABLE dlh.oil_dds.hub_production_day (production_hk varchar,production_id varchar,load_dts timestamp(6) with time zone,record_source varchar) WITH (format = 'PARQUET');

CREATE TABLE dlh.oil_dds.link_well_field (link_hk varchar,well_hk varchar,field_hk varchar,load_dts timestamp(6) with time zone,record_source varchar) WITH (format = 'PARQUET');

CREATE TABLE dlh.oil_dds.link_production_day_well (link_hk varchar,production_hk varchar,well_hk varchar,load_dts timestamp(6) with time zone,record_source varchar) WITH (format = 'PARQUET');

CREATE TABLE dlh.oil_dds.sat_field (field_hk varchar,event_id varchar,entity_version bigint,op varchar,event_time timestamp(6) with time zone,load_dts timestamp(6) with time zone,record_source varchar,hashdiff varchar,field_name varchar,region varchar) WITH (format = 'PARQUET');

CREATE TABLE dlh.oil_dds.sat_well (well_hk varchar,link_hk varchar,event_id varchar,entity_version bigint,op varchar,event_time timestamp(6) with time zone,load_dts timestamp(6) with time zone,record_source varchar,hashdiff varchar,well_name varchar,valid_from date,status varchar) WITH (format = 'PARQUET');

CREATE TABLE dlh.oil_dds.sat_daily_production (production_hk varchar,link_hk varchar,event_id varchar,entity_version bigint,op varchar,event_time timestamp(6) with time zone,load_dts timestamp(6) with time zone,record_source varchar,hashdiff varchar,production_date date,oil_tonnes numeric(18,3),status varchar) WITH (format = 'PARQUET');

CREATE TABLE dlh.oil_mart.dim_field (field_hk varchar,entity_version bigint,valid_from timestamp(6) with time zone,valid_to timestamp(6) with time zone,field_name varchar,region varchar,is_deleted boolean) WITH (format = 'PARQUET');

CREATE TABLE dlh.oil_mart.dim_well (well_hk varchar,field_hk varchar,entity_version bigint,valid_from date,valid_to date,well_name varchar,status varchar,is_deleted boolean) WITH (format = 'PARQUET');

CREATE TABLE dlh.oil_mart.fact_well_day (production_hk varchar,well_hk varchar,field_hk varchar,production_date date,well_version bigint,field_version bigint,status varchar,oil_tonnes numeric(18,3)) WITH (format = 'PARQUET');

CREATE TABLE dlh.oil_mart.agg_field_day (production_date date,field_hk varchar,field_name varchar,region varchar,oil_tonnes numeric(22,3),producing_well_days bigint,oil_per_well_day numeric(22,6)) WITH (format = 'PARQUET');
