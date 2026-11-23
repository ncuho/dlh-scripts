-- Хранит историю связи well_field через hash keys связанных hubs.
CREATE TABLE dlh.oil_r_example.link_well_field WITH (
    format = 'PARQUET'
) AS SELECT DISTINCT
    lower(
        to_hex(
            md5(
                to_utf8 (
                    'well_field:' || entity_id || ':' || json_extract_scalar (
                        document, '$.payload.field_id'
)
)
)
)
) AS link_hk,
    lower(
        to_hex(
            md5(
                to_utf8 (
                    'well:' || entity_id
)
)
)
) AS well_hk,
    lower(
        to_hex(
            md5(
                to_utf8 (
                    'field:' || json_extract_scalar (
                        document, '$.payload.field_id'
)
)
)
)
) AS field_hk
FROM
    dlh.oil_r_example.canonical_event
WHERE
    event_type = 'well';
