-- PLAN ONLY: выполните перед миграцией только после проверки коннектора и прав.
-- PK/FK в целевой Iceberg здесь не декларируются; инварианты проверяются отдельно.
CREATE SCHEMA IF NOT EXISTS dlh.commerce_stg;
CREATE SCHEMA IF NOT EXISTS dlh.commerce_ods;
CREATE SCHEMA IF NOT EXISTS dlh.commerce_vault;
CREATE SCHEMA IF NOT EXISTS dlh.commerce_dm;

CREATE TABLE dlh.commerce_stg.kafka_event (
    topic VARCHAR,
    kafka_partition INTEGER,
    kafka_offset BIGINT,
    batch_id BIGINT,
    received_at TIMESTAMP(6) WITH TIME ZONE,
    event_json VARCHAR
) WITH (format = 'PARQUET');

CREATE TABLE dlh.commerce_stg.batch_control (
    batch_id BIGINT,
    source_name VARCHAR,
    created_at TIMESTAMP(6) WITH TIME ZONE,
    status VARCHAR,
    source_manifest VARCHAR,
    error_text VARCHAR
) WITH (format = 'PARQUET');

CREATE TABLE dlh.commerce_ods.event (
    event_id VARCHAR,
    event_type VARCHAR,
    entity_id VARCHAR,
    entity_version BIGINT,
    op VARCHAR,
    event_time TIMESTAMP(6) WITH TIME ZONE,
    load_dts TIMESTAMP(6) WITH TIME ZONE,
    record_source VARCHAR,
    batch_id BIGINT,
    payload VARCHAR,
    event_document VARCHAR
) WITH (format = 'PARQUET');

CREATE TABLE dlh.commerce_vault.hub_product (
    product_hk VARCHAR,
    product_id VARCHAR,
    load_dts TIMESTAMP(6) WITH TIME ZONE,
    record_source VARCHAR
) WITH (format = 'PARQUET');

CREATE TABLE dlh.commerce_vault.hub_store (
    store_hk VARCHAR,
    store_id VARCHAR,
    load_dts TIMESTAMP(6) WITH TIME ZONE,
    record_source VARCHAR
) WITH (format = 'PARQUET');

CREATE TABLE dlh.commerce_vault.hub_shipment (
    shipment_hk VARCHAR,
    shipment_id VARCHAR,
    load_dts TIMESTAMP(6) WITH TIME ZONE,
    record_source VARCHAR
) WITH (format = 'PARQUET');

CREATE TABLE dlh.commerce_vault.hub_return (
    return_hk VARCHAR,
    return_id VARCHAR,
    load_dts TIMESTAMP(6) WITH TIME ZONE,
    record_source VARCHAR
) WITH (format = 'PARQUET');

CREATE TABLE dlh.commerce_vault.link_shipment (
    link_hk VARCHAR,
    shipment_hk VARCHAR,
    product_hk VARCHAR,
    store_hk VARCHAR,
    load_dts TIMESTAMP(6) WITH TIME ZONE,
    record_source VARCHAR
) WITH (format = 'PARQUET');

CREATE TABLE dlh.commerce_vault.link_return (
    link_hk VARCHAR,
    return_hk VARCHAR,
    shipment_hk VARCHAR,
    load_dts TIMESTAMP(6) WITH TIME ZONE,
    record_source VARCHAR
) WITH (format = 'PARQUET');

CREATE TABLE dlh.commerce_vault.sat_product (
    product_hk VARCHAR,
    event_id VARCHAR,
    entity_version BIGINT,
    op VARCHAR,
    event_time TIMESTAMP(6) WITH TIME ZONE,
    load_dts TIMESTAMP(6) WITH TIME ZONE,
    record_source VARCHAR,
    hashdiff VARCHAR,
    product_name VARCHAR,
    category VARCHAR
) WITH (format = 'PARQUET');

CREATE TABLE dlh.commerce_vault.sat_store (
    store_hk VARCHAR,
    event_id VARCHAR,
    entity_version BIGINT,
    op VARCHAR,
    event_time TIMESTAMP(6) WITH TIME ZONE,
    load_dts TIMESTAMP(6) WITH TIME ZONE,
    record_source VARCHAR,
    hashdiff VARCHAR,
    store_name VARCHAR,
    region VARCHAR
) WITH (format = 'PARQUET');

CREATE TABLE dlh.commerce_vault.sat_shipment (
    shipment_hk VARCHAR,
    link_hk VARCHAR,
    event_id VARCHAR,
    entity_version BIGINT,
    op VARCHAR,
    event_time TIMESTAMP(6) WITH TIME ZONE,
    load_dts TIMESTAMP(6) WITH TIME ZONE,
    record_source VARCHAR,
    hashdiff VARCHAR,
    order_id VARCHAR,
    business_date DATE,
    quantity BIGINT,
    unit_price DECIMAL(18,2),
    currency VARCHAR
) WITH (format = 'PARQUET');

CREATE TABLE dlh.commerce_vault.sat_return (
    return_hk VARCHAR,
    link_hk VARCHAR,
    event_id VARCHAR,
    entity_version BIGINT,
    op VARCHAR,
    event_time TIMESTAMP(6) WITH TIME ZONE,
    load_dts TIMESTAMP(6) WITH TIME ZONE,
    record_source VARCHAR,
    hashdiff VARCHAR,
    business_date DATE,
    quantity BIGINT,
    unit_price DECIMAL(18,2),
    currency VARCHAR
) WITH (format = 'PARQUET');

CREATE TABLE dlh.commerce_dm.dim_product (
    product_hk VARCHAR,
    entity_version BIGINT,
    valid_from TIMESTAMP(6) WITH TIME ZONE,
    valid_to TIMESTAMP(6) WITH TIME ZONE,
    product_name VARCHAR,
    category VARCHAR,
    is_deleted BOOLEAN
) WITH (format = 'PARQUET');

CREATE TABLE dlh.commerce_dm.dim_store (
    store_hk VARCHAR,
    entity_version BIGINT,
    valid_from TIMESTAMP(6) WITH TIME ZONE,
    valid_to TIMESTAMP(6) WITH TIME ZONE,
    store_name VARCHAR,
    region VARCHAR,
    is_deleted BOOLEAN
) WITH (format = 'PARQUET');

CREATE TABLE dlh.commerce_dm.fact_shipment (
    shipment_hk VARCHAR,
    shipment_id VARCHAR,
    product_hk VARCHAR,
    store_hk VARCHAR,
    product_version BIGINT,
    store_version BIGINT,
    business_date DATE,
    quantity BIGINT,
    unit_price DECIMAL(18,2),
    currency VARCHAR,
    amount DECIMAL(20,2)
) WITH (format = 'PARQUET');

CREATE TABLE dlh.commerce_dm.fact_return (
    return_hk VARCHAR,
    return_id VARCHAR,
    shipment_hk VARCHAR,
    product_hk VARCHAR,
    store_hk VARCHAR,
    product_version BIGINT,
    store_version BIGINT,
    business_date DATE,
    quantity BIGINT,
    unit_price DECIMAL(18,2),
    currency VARCHAR,
    amount DECIMAL(20,2)
) WITH (format = 'PARQUET');

CREATE TABLE dlh.commerce_dm.sales_day (
    business_date DATE,
    region VARCHAR,
    category VARCHAR,
    currency VARCHAR,
    shipment_amount DECIMAL(22,2),
    return_amount DECIMAL(22,2),
    net_amount DECIMAL(22,2),
    shipped_qty BIGINT,
    returned_qty BIGINT
) WITH (format = 'PARQUET');
