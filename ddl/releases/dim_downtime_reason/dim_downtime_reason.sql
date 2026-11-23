-- Определяет справочник причин простоя для размерной модели.
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
