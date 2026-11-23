# gold.agg_field_day

Дневная витрина зерна «месторождение × дата»: тонны, активные скважино-дни и тонны на активный скважино-день.

`agg_field_day.sql` создаёт пустую INITIAL-таблицу; `refresh.sql` — отдельного кандидата `_next`. Загрузку и проверку выполняет task `oil_mart__agg_field_day` DAG `oil_layered_migration`. После частичного сбоя не повторяйте INSERT без сверки receipt.
