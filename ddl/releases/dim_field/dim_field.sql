-- Назначение: Историческое измерение месторождения. Интервал действия версии — [valid_from, valid_to); удаление сохраняется отдельным признаком.
-- Исполняйте в указанном SQL-движке; одноимённые функции и типы GP и Spark могут иметь разную семантику.
-- Review template: replace oil_r_example with the chosen immutable release.
-- Executed parameterized equivalent: pipelines/sql_plan/sql_plan.py.
CREATE TABLE dlh.oil_r_example.dim_field WITH (
    format = 'PARQUET'
) AS
SELECT
    lower(
        to_hex(
            md5(
                to_utf8 (
                    'field:' || entity_id
)
)
)
) AS field_hk,
    entity_version,
    event_time AS valid_from,
    lead(
        event_time) OVER (PARTITION BY entity_id ORDER BY event_time, entity_version, event_id) AS valid_to,
    json_extract_scalar (document, '$.payload.field_name') AS field_name,
    json_extract_scalar (document, '$.payload.region') AS region,
    (op = 'D') AS is_deleted
FROM
    dlh.oil_r_example.canonical_event
WHERE
    event_type = 'field';
