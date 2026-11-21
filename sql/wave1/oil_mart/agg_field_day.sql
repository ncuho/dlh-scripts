CREATE TABLE dlh.oil_mart.agg_field_day WITH (
    format = 'PARQUET'
) AS
SELECT
    p.production_date,
    p.field_hk,
    f.field_name,
    f.region,
    CAST(
        sum(
            p.oil_tonnes
) AS DECIMAL(
            22, 3
)
) AS oil_tonnes,
    CAST(
        sum(
            CASE WHEN p.status = 'ACTIVE' THEN
                1
            ELSE
                0
            END
) AS BIGINT
) AS producing_well_days,
    CASE WHEN sum(
        CASE WHEN p.status = 'ACTIVE' THEN
            1
        ELSE
            0
        END) = 0 THEN
        CAST(NULL AS DECIMAL(22, 6))
    ELSE
        CAST(sum(p.oil_tonnes) / sum(
                CASE WHEN p.status = 'ACTIVE' THEN
                    1
                ELSE
                    0
                END) AS DECIMAL(22, 6))
    END AS oil_per_well_day
FROM
    dlh.oil_mart.fact_well_day p
    JOIN dlh.oil_mart.dim_field f ON f.field_hk = p.field_hk
        AND f.entity_version = p.field_version
GROUP BY
    p.production_date,
    p.field_hk,
    f.field_name,
    f.region;
