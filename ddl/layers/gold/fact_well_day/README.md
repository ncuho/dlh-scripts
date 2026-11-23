# gold.fact_well_day

Факт зерна «скважина × производственная дата». Содержит тонны, статус и версии измерений well/field, действовавшие на дату.

`fact_well_day.sql` создаёт пустую INITIAL-таблицу; `refresh.sql` — отдельного кандидата `_next`. Загрузку и проверку выполняет task `oil_mart__fact_well_day` DAG `oil_layered_migration`. После частичного сбоя не повторяйте INSERT без сверки receipt.
