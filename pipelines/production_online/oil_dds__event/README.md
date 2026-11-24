# oil_dds__event

Реестр принятых событий DDS. event_id определяет повтор, entity_version задаёт порядок изменений, event_document позволяет сверить историю.

Источник — `oil_dds.event`, целевая Iceberg-таблица — `dlh.silver.event`. Имена и UUID сохраняются при догоне.

`ddl.sql` создаёт объект; `initial.sql` задаёт прямую копию согласованного GP C0 через Cedrus без Spark. `checks/` хранит проверки ключей, ссылок и данных. Все 20 объектов проверяет одна task migrate_and_validate в oil_layered_migration.

Исполняемая Spark-обработка — `pipelines/oil_dlh_release/oil_dds__event/apply.sql`: MERGE Bronze/Silver либо пересчёт Gold, с предыдущими snapshots и ограниченным окном raw. Её вызывают oil_dlh_release в режимах catchup и release. Здесь `refresh.sql` сохранён как исходный шаблон переноса логики; поверх живых таблиц вручную его не запускают.

Ключи, Decimal, valid-time и удаления сохраняются; перенос GP-ограничений реализован явными DQ-gates и порядком родителей. Не заменяйте сверку значений одним COUNT.
