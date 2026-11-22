CREATE TABLE dlh.oil_mart.fact_well_day WITH (
    format = 'PARQUET'
) AS
SELECT
    p.production_hk,
    l.well_hk,
    w.field_hk,
    p.production_date,
    w.entity_version AS well_version,
    f.entity_version AS field_version,
    p.status,
    p.oil_tonnes
FROM (
    SELECT
        *,
        row_number(
) OVER ( PARTITION BY production_hk ORDER BY entity_version DESC,
        event_id DESC
) rn
FROM dlh.oil_dds.sat_daily_production) p
JOIN dlh.oil_dds.link_production_day_well l ON l.link_hk = p.link_hk
JOIN dlh.oil_mart.dim_well w ON w.well_hk = l.well_hk
    AND NOT w.is_deleted
    AND w.valid_from <= p.production_date
    AND (w.valid_to IS NULL
        OR p.production_date < w.valid_to)
JOIN dlh.oil_mart.dim_field f ON f.field_hk = w.field_hk
    AND NOT f.is_deleted
    AND f.valid_from <= (CAST(p.production_date AS timestamp) AT TIME ZONE 'UTC')
    AND (f.valid_to IS NULL
        OR (CAST(p.production_date AS timestamp) AT TIME ZONE 'UTC') < f.valid_to)
WHERE
    p.rn = 1
    AND p.op = 'U';
