# oil_stg__event_log

Принимает исходные JSON-события вместе с topic, partition, offset и ID партии. Сохраняет доставку до применения бизнес-логики.

Источник — `oil_stg.event_log`, целевая Iceberg-таблица — `dlh.bronze.event_log`. Имена и UUID сохраняются при догоне.

`ddl.sql` создаёт объект; `initial.sql` задаёт прямую копию согласованного GP C0 через Cedrus без Spark. `checks/` хранит проверки ключей, ссылок и данных. Все 20 объектов проверяет одна task migrate_and_validate в oil_layered_migration.

Исполняемая Spark-обработка — `pipelines/oil_dlh_release/oil_stg__event_log/apply.sql`: MERGE Bronze/Silver либо пересчёт Gold, с предыдущими snapshots и ограниченным окном raw. Её вызывают oil_dlh_release в режимах catchup и release. Здесь `refresh.sql` сохранён как исходный шаблон переноса логики; поверх живых таблиц вручную его не запускают.

Ключи, Decimal, valid-time и удаления сохраняются; перенос GP-ограничений реализован явными DQ-gates и порядком родителей. Не заменяйте сверку значений одним COUNT.
