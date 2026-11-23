# gold.v_field_production

Serving-представление первой цепочки поверх agg_field_day для отчёта добычи.

`v_field_production.sql` создаёт пустую INITIAL-таблицу; `refresh.sql` — отдельного кандидата `_next`. Загрузку и проверку выполняет task `oil_mart__v_field_production` DAG `oil_layered_migration`. После частичного сбоя не повторяйте INSERT без сверки receipt.
