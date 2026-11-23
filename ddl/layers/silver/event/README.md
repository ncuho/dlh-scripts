# silver.event

Реестр принятых событий DDS. event_id определяет повтор, entity_version задаёт порядок изменений, event_document позволяет сверить историю.

`event.sql` создаёт пустую INITIAL-таблицу; `refresh.sql` — отдельного кандидата `_next`. Загрузку и проверку выполняет task `oil_dds__event` DAG `oil_layered_migration`. После частичного сбоя не повторяйте INSERT без сверки receipt.
