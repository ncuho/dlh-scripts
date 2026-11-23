-- Назначение: Факт зерна «скважина × производственная дата». Содержит тонны, статус и версии измерений well/field, действовавшие на дату.
-- Исполняйте в указанном SQL-движке; одноимённые функции и типы GP и Spark могут иметь разную семантику.
-- Review template: replace oil_r_example with the chosen immutable release.
-- Executed parameterized equivalent: pipelines/sql_plan/sql_plan.py.
CREATE TABLE dlh.oil_r_example.fact_well_day WITH (
    format = 'PARQUET'
) AS
WITH p AS (
    SELECT
        *,
        row_number() OVER (PARTITION BY entity_id ORDER BY entity_version DESC,
            event_id DESC) AS rn
    FROM
        dlh.oil_r_example.canonical_event
    WHERE
        event_type = 'production_day'
),
decoded AS (
    SELECT
        lower(to_hex(md5(to_utf8 ('production:' || entity_id)))) AS production_hk,
        lower(to_hex(md5(to_utf8 ('well:' || json_extract_scalar (document, '$.payload.well_id'))))) AS well_hk,
        CAST(json_extract_scalar (document, '$.payload.production_date') AS date) AS production_date,
        CAST(json_extract_scalar (document, '$.payload.oil_tonnes') AS DECIMAL(18, 3)) AS oil_tonnes,
        json_extract_scalar (document, '$.payload.status') AS status
    FROM
        p
    WHERE
        rn = 1
        AND op = 'U'
)
SELECT
    p.production_hk,
    p.well_hk,
    w.field_hk,
    p.production_date,
    w.entity_version AS well_version,
    f.entity_version AS field_version,
    p.status,
    p.oil_tonnes
FROM
    decoded p
    JOIN dlh.oil_r_example.dim_well w ON w.well_hk = p.well_hk
        AND NOT w.is_deleted
        AND w.valid_from <= p.production_date
        AND (w.valid_to IS NULL
            OR p.production_date < w.valid_to)
    JOIN dlh.oil_r_example.dim_field f ON f.field_hk = w.field_hk
        AND NOT f.is_deleted
        AND f.valid_from <= (CAST(p.production_date AS timestamp) AT TIME ZONE 'UTC')
        AND (f.valid_to IS NULL
            OR (CAST(p.production_date AS timestamp) AT TIME ZONE 'UTC') < f.valid_to);
