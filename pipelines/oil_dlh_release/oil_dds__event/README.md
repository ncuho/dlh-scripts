# oil_dds__event

`apply.sql` строит полное состояние существующей таблицы в Spark. Предыдущий выпуск читается по закреплённому Iceberg snapshot; новые события ограничены offsets текущего выпуска. Бизнес-ключи сохраняются, новые технические партии имеют отрицательный batch_id.

Родители: oil_stg__v_event_parsed, oil_stg__batch_control. Проверки в `checks/` выполняются перед допуском детей. `oil_dlh_release` в режиме `catchup` дополнительно сравнивает Gold с GP C1; `oil_dlh_release` работает без чтения GP.
