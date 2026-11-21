-- Review template: replace oil_r_example with the chosen immutable release.
-- Executed parameterized equivalent: runtime/sql_plan.py.
CREATE TABLE dlh.oil_r_example.hub_downtime WITH (
    format = 'PARQUET'
) AS SELECT DISTINCT
    lower(
        to_hex(
            md5(
                to_utf8 (
                    'downtime:' || entity_id
)
)
)
) AS downtime_hk,
    entity_id
FROM
    dlh.oil_r_example.canonical_event
WHERE
    event_type = 'downtime';
