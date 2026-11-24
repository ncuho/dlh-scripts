# oil_mart__fact_well_day

`apply.sql` строит полное состояние существующей таблицы в Spark. Предыдущий выпуск читается по закреплённому Iceberg snapshot; новые события ограничены offsets текущего выпуска. Бизнес-ключи сохраняются, новые технические партии имеют отрицательный batch_id.

Родители: oil_dds__link_production_day_well, oil_dds__sat_daily_production, oil_mart__dim_field, oil_mart__dim_well. Проверки в `checks/` выполняются перед допуском детей. `oil_dlh_catchup` дополнительно сравнивает Gold с GP C1; `oil_dlh_release` работает без чтения GP.
