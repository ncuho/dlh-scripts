# bronze.event_log

Принимает исходные JSON-события вместе с topic, partition, offset и ID партии. Сохраняет доставку до применения бизнес-логики.

`event_log.sql` создаёт пустую INITIAL-таблицу; `refresh.sql` — отдельного кандидата `_next`. Загрузку и проверку выполняет task `oil_stg__event_log` DAG `oil_layered_migration`. После частичного сбоя не повторяйте INSERT без сверки receipt.
