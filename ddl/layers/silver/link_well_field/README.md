# silver.link_well_field

Хранит историческую связь скважины с месторождением по техническим ключам. Изменяемые атрибуты находятся в satellites.

`link_well_field.sql` создаёт пустую INITIAL-таблицу; `refresh.sql` — отдельного кандидата `_next`. Загрузку и проверку выполняет task `oil_dds__link_well_field` DAG `oil_layered_migration`. После частичного сбоя не повторяйте INSERT без сверки receipt.
