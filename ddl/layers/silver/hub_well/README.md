# silver.hub_well

Хранит бизнес-идентификатор скважины и детерминированный hash key. Не содержит изменяемых описательных атрибутов.

`hub_well.sql` создаёт пустую INITIAL-таблицу; `refresh.sql` — отдельного кандидата `_next`. Загрузку и проверку выполняет task `oil_dds__hub_well` DAG `oil_layered_migration`. После частичного сбоя не повторяйте INSERT без сверки receipt.
