# bronze.v_event_parsed

Разбирает envelope JSON в STG: тип, ID, версию, операцию, время и payload без изменения исходного документа.

`v_event_parsed.sql` создаёт пустую INITIAL-таблицу; `refresh.sql` — отдельного кандидата `_next`. Загрузку и проверку выполняет task `oil_stg__v_event_parsed` DAG `oil_layered_migration`. После частичного сбоя не повторяйте INSERT без сверки receipt.
