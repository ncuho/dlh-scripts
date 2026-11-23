# silver.link_production_day_well

Хранит историческую связь скважино-дня со скважиной по техническим ключам. Изменяемые атрибуты находятся в satellites.

`link_production_day_well.sql` создаёт пустую INITIAL-таблицу; `refresh.sql` — отдельного кандидата `_next`. Загрузку и проверку выполняет task `oil_dds__link_production_day_well` DAG `oil_layered_migration`. После частичного сбоя не повторяйте INSERT без сверки receipt.
