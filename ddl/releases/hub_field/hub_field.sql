-- Хранит устойчивый бизнес-ключ и hash key сущности field; атрибуты вынесены в satellites.
CREATE TABLE dlh.oil_r_example.hub_field WITH (
    format = 'PARQUET'
) AS SELECT DISTINCT
    lower(
        to_hex(
            md5(
                to_utf8 (
                    'field:' || entity_id
)
)
)
) AS field_hk,
    entity_id
FROM
    dlh.oil_r_example.canonical_event
WHERE
    event_type = 'field';
