# bronze.batch_control

Хранит жизненный цикл партии: OPEN, READY, COALESCED, APPLIED или FAILED. Отделяет успешную доставку от применения в DDS.

`batch_control.sql` создаёт пустую INITIAL-таблицу; `refresh.sql` — отдельного кандидата `_next`. Загрузку и проверку выполняет task `oil_stg__batch_control` DAG `oil_layered_migration`. После частичного сбоя не повторяйте INSERT без сверки receipt.
