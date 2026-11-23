-- Контекст миграции: AS IS / TO BE.
-- Назначение: Формирует SCD2-версии скважин и принадлежность месторождению.
-- AS IS: История событий и GP-расчёты обеих исходных веток.
-- TO BE: Альтернативная release-проекция с canonical_input/event и размерными моделями.
-- Отличие: Сохранённый маршрут; production_online первой волны имеет собственный 20-объектный контракт.
-- Конец контекста миграции.
-- Review template: replace oil_r_example with the chosen immutable release.
-- Executed parameterized equivalent: pipelines/sql_plan/sql_plan.py.
CREATE TABLE dlh.oil_r_example.dim_well WITH (
    format = 'PARQUET'
) AS
SELECT
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
) AS field_hk,
    entity_version,
    CAST(
        json_extract_scalar (
            document, '$.payload.valid_from'
) AS DATE
) AS valid_from,
    lead(
        CAST(
            json_extract_scalar (
                document, '$.payload.valid_from'
) AS DATE
)) OVER (PARTITION BY entity_id ORDER BY CAST(json_extract_scalar (document, '$.payload.valid_from') AS date),
        entity_version,
        event_id) AS valid_to,
    json_extract_scalar (document, '$.payload.well_name') AS well_name,
    json_extract_scalar (document, '$.payload.status') AS status,
    (op = 'D') AS is_deleted
FROM
    dlh.oil_r_example.canonical_event
WHERE
    event_type = 'well';
