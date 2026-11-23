# silver.hub_production_day

Хранит бизнес-идентификатор скважино-дня добычи и детерминированный hash key. Не содержит изменяемых описательных атрибутов.

`hub_production_day.sql` создаёт пустую INITIAL-таблицу; `refresh.sql` — отдельного кандидата `_next`. Загрузку и проверку выполняет task `oil_dds__hub_production_day` DAG `oil_layered_migration`. После частичного сбоя не повторяйте INSERT без сверки receipt.
