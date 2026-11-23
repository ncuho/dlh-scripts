# silver.sat_well

Сохраняет версии атрибутов скважины, event_id, entity_version и операцию U/D. GP хранит типизированный payload и audit-поля; release DLH использует логическую проекцию с исходным document.

`sat_well.sql` создаёт пустую INITIAL-таблицу; `refresh.sql` — отдельного кандидата `_next`. Загрузку и проверку выполняет task `oil_dds__sat_well` DAG `oil_layered_migration`. После частичного сбоя не повторяйте INSERT без сверки receipt.
