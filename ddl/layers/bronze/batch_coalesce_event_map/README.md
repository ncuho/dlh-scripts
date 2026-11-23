# bronze.batch_coalesce_event_map

Сохраняет соответствие каждой Kafka-позиции исходной и объединённой партиям.

`batch_coalesce_event_map.sql` создаёт пустую INITIAL-таблицу; `refresh.sql` — отдельного кандидата `_next`. Загрузку и проверку выполняет task `oil_stg__batch_coalesce_event_map` DAG `oil_layered_migration`. После частичного сбоя не повторяйте INSERT без сверки receipt.
