-- Назначение: Историческое измерение месторождения. Интервал действия версии — [valid_from, valid_to); удаление сохраняется отдельным признаком.
-- Исполняйте в указанном SQL-движке; одноимённые функции и типы GP и Spark могут иметь разную семантику.
CREATE TABLE dlh.oil_mart.dim_field WITH (
    format = 'PARQUET'
) AS
SELECT
    field_hk,
    entity_version,
    event_time AS valid_from,
    lead(
        event_time) OVER (PARTITION BY field_hk ORDER BY event_time, entity_version) AS valid_to,
    field_name,
    region,
    (op = 'D') AS is_deleted
FROM
    dlh.oil_dds.sat_field;
