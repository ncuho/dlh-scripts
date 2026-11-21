-- Review template: replace oil_r_example with the chosen immutable release.
-- Executed parameterized equivalent: runtime/sql_plan.py.
CREATE TABLE dlh.oil_r_example.link_production_day_well WITH (
    format = 'PARQUET'
) AS SELECT DISTINCT
    lower(
        to_hex(
            md5(
                to_utf8 (
                    'production_day_well:' || entity_id || ':' || json_extract_scalar (
                        document, '$.payload.well_id'
)
)
)
)
) AS link_hk,
    lower(
        to_hex(
            md5(
                to_utf8 (
                    'production:' || entity_id
)
)
)
) AS production_hk,
    lower(
        to_hex(
            md5(
                to_utf8 (
                    'well:' || json_extract_scalar (
                        document, '$.payload.well_id'
)
)
)
)
) AS well_hk
FROM
    dlh.oil_r_example.canonical_event
WHERE
    event_type = 'production_day';
