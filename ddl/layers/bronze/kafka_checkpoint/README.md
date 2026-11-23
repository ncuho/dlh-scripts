# bronze.kafka_checkpoint

Хранит следующий ожидаемый offset каждой партиции. Обновляется в одной транзакции с записью события в GP.

`kafka_checkpoint.sql` создаёт пустую INITIAL-таблицу; `refresh.sql` — отдельного кандидата `_next`. Загрузку и проверку выполняет task `oil_stg__kafka_checkpoint` DAG `oil_layered_migration`. После частичного сбоя не повторяйте INSERT без сверки receipt.
