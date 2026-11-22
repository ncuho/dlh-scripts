CREATE TABLE dlh.oil_r_example.canonical_event WITH (
    format = 'PARQUET'
) AS
SELECT
    document,
    json_extract_scalar (
        document, '$.event_id'
) AS event_id,
    json_extract_scalar (
        document, '$.event_type'
) AS event_type,
    json_extract_scalar (
        document, '$.entity_id'
) AS entity_id,
    CAST(
        json_extract_scalar (
            document, '$.entity_version'
) AS BIGINT
) AS entity_version,
    json_extract_scalar (
        document, '$.op'
) AS op,
    from_iso8601_timestamp (
        json_extract_scalar (
            document, '$.event_time'
)
) AS event_time
FROM (
    SELECT DISTINCT
        document
    FROM
        dlh.oil_r_example.canonical_input);
