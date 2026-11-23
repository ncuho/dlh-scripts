-- Хранит версии атрибутов downtime, происхождение, порядок версий и операции изменения.
CREATE TABLE dlh.oil_r_example.sat_downtime WITH (
    format = 'PARQUET'
) AS
SELECT
    lower(
        to_hex(
            md5(
                to_utf8 (
                    'downtime:' || entity_id
)
)
)
) AS downtime_hk,
    event_id,
    entity_version,
    op,
    event_time,
    document
FROM
    dlh.oil_r_example.canonical_event
WHERE
    event_type = 'downtime';
