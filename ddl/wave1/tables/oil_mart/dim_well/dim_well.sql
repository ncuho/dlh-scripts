-- Назначение: Историческое измерение скважины, её принадлежности месторождению и статуса. Версия выбирается на производственную дату.
-- Исполняйте в указанном SQL-движке; одноимённые функции и типы GP и Spark могут иметь разную семантику.
CREATE TABLE dlh.oil_mart.dim_well WITH (
    format = 'PARQUET'
) AS
SELECT
    w.well_hk,
    l.field_hk,
    w.entity_version,
    w.valid_from,
    lead(
        w.valid_from) OVER (PARTITION BY w.well_hk ORDER BY w.valid_from, w.entity_version) AS valid_to,
    w.well_name,
    w.status,
    (w.op = 'D') AS is_deleted
FROM
    dlh.oil_dds.sat_well w
    JOIN dlh.oil_dds.link_well_field l ON l.link_hk = w.link_hk;
