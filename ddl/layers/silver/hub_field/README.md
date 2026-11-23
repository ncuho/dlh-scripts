# silver.hub_field

Хранит бизнес-идентификатор месторождения и детерминированный hash key. Не содержит изменяемых описательных атрибутов.

`hub_field.sql` создаёт пустую INITIAL-таблицу; `refresh.sql` — отдельного кандидата `_next`. Загрузку и проверку выполняет task `oil_dds__hub_field` DAG `oil_layered_migration`. После частичного сбоя не повторяйте INSERT без сверки receipt.
