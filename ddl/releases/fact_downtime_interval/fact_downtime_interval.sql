-- Назначение: Факт зерна «интервал простоя». Содержит скважину, месторождение, начало/конец, причину и длительность в часах.
-- Исполняйте в указанном SQL-движке; одноимённые функции и типы GP и Spark могут иметь разную семантику.
-- Review template: replace oil_r_example with the chosen immutable release.
-- Executed parameterized equivalent: pipelines/sql_plan/sql_plan.py.
CREATE TABLE dlh.oil_r_example.fact_downtime_interval WITH (
    format = 'PARQUET'
) AS
WITH d AS (
    SELECT
        *,
        row_number() OVER (PARTITION BY entity_id ORDER BY entity_version DESC,
            event_id DESC) AS rn
    FROM
        dlh.oil_r_example.canonical_event
    WHERE
        event_type = 'downtime'
),
decoded AS (
    SELECT
        lower(to_hex(md5(to_utf8 ('downtime:' || entity_id)))) AS downtime_hk,
        lower(to_hex(md5(to_utf8 ('well:' || json_extract_scalar (document, '$.payload.well_id'))))) AS well_hk,
        from_iso8601_timestamp (json_extract_scalar (document, '$.payload.start_at')) AS start_at,
        from_iso8601_timestamp (json_extract_scalar (document, '$.payload.end_at')) AS end_at,
        json_extract_scalar (document, '$.payload.reason') AS reason
    FROM
        d
    WHERE
        rn = 1
        AND op = 'U'
)
SELECT
    d.downtime_hk,
    d.well_hk,
    p.field_hk,
    d.start_at,
    d.end_at,
    d.reason,
    CAST((to_unixtime (d.end_at) - to_unixtime (d.start_at)) / 3600 AS DECIMAL(16, 6)) AS hours
FROM
    decoded d
    JOIN dlh.oil_r_example.fact_well_day p ON p.well_hk = d.well_hk
        AND p.production_date = CAST(at_timezone (d.start_at, 'UTC') AS DATE)
        AND p.status = 'ACTIVE';
