-- Назначение: Справочник причин: EQUIPMENT, POWER, PLANNED. Нормализует группировку простоев.
-- Исполняйте в указанном SQL-движке; одноимённые функции и типы GP и Spark могут иметь разную семантику.
-- Review template: replace oil_r_example with the chosen immutable release.
-- Executed parameterized equivalent: pipelines/sql_plan/sql_plan.py.
CREATE TABLE dlh.oil_r_example.dim_downtime_reason WITH (
    format = 'PARQUET'
) AS
SELECT
    reason,
    description
FROM (
    VALUES (
            'EQUIPMENT', 'Оборудование'
),
        (
            'POWER', 'Энергоснабжение'
),
        (
            'PLANNED', 'Плановый простой'
)
) AS v (
        reason, description);
