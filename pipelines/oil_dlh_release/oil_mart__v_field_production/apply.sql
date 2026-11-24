-- Обновляет существующий объект из предыдущего принятого snapshot и ограниченного окна raw; _next не создаётся.
-- Проецируем итоговый агрегат в контракт колонок BI; source-view здесь материализуется как таблица выпуска. Расчёт выполняется вместо соответствующей части refresh_production GP.
SELECT production_date,
       field_hk,
       field_name,
       region,
       oil_tonnes,
       producing_well_days,
       oil_per_well_day
FROM {{target:oil_mart__agg_field_day}};
