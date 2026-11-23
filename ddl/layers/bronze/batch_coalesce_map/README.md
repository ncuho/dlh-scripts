# bronze.batch_coalesce_map

Связывает исходные партии с объединённой партией и сохраняет число исходных доставок.

`batch_coalesce_map.sql` создаёт пустую INITIAL-таблицу; `refresh.sql` — отдельного кандидата `_next`. Загрузку и проверку выполняет task `oil_stg__batch_coalesce_map` DAG `oil_layered_migration`. После частичного сбоя не повторяйте INSERT без сверки receipt.
