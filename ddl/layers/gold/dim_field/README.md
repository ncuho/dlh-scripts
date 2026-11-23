# gold.dim_field

Историческое измерение месторождения. Интервал действия версии — [valid_from, valid_to); удаление сохраняется отдельным признаком.

`dim_field.sql` создаёт пустую INITIAL-таблицу; `refresh.sql` — отдельного кандидата `_next`. Загрузку и проверку выполняет task `oil_mart__dim_field` DAG `oil_layered_migration`. После частичного сбоя не повторяйте INSERT без сверки receipt.
