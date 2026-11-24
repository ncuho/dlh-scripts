# oil_dds__sat_well

Сохраняет версии атрибутов скважины, event_id, entity_version и операцию U/D. GP хранит типизированный payload и audit-поля; release DLH использует логическую проекцию с исходным document.

Источник — `oil_dds.sat_well`, целевая Iceberg-таблица — `dlh.silver.sat_well`. Имена и UUID сохраняются при догоне.

`ddl.sql` создаёт объект; `initial.sql` задаёт прямую копию согласованного GP C0 через Cedrus без Spark. `checks/` хранит проверки ключей, ссылок и данных. Все 20 объектов проверяет одна task migrate_and_validate в oil_layered_migration.

Исполняемая Spark-обработка — `pipelines/oil_dlh_release/oil_dds__sat_well/apply.sql`: MERGE Bronze/Silver либо пересчёт Gold, с предыдущими snapshots и ограниченным окном raw. Её вызывают oil_dlh_catchup и oil_dlh_release. Здесь `refresh.sql` сохранён как исходный шаблон переноса логики; поверх живых таблиц вручную его не запускают.

Ключи, Decimal, valid-time и удаления сохраняются; перенос GP-ограничений реализован явными DQ-gates и порядком родителей. Не заменяйте сверку значений одним COUNT.
