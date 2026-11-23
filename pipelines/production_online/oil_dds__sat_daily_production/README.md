# oil_dds__sat_daily_production

Сохраняет версии атрибутов дневной добычи, event_id, entity_version и операцию U/D. GP хранит типизированный payload и audit-поля; release DLH использует логическую проекцию с исходным document.

Источник: `oil_dds.sat_daily_production`. Целевая relation — физическая Iceberg таблица immutable release; для исходного view материализация заменяет runtime-view, бизнес-контракт сохраняется.

`ddl.sql` создаёт пустой объект через Cedrus/Trino. `initial.sql` — trino SELECT для C0. `refresh.sql` — spark SELECT для нового выпуска. `checks.sql` содержит блокирующие проверки с явной phase; исполняйте только проверки нужной фазы и both.

Зависимости INITIAL: исходный capture. REFRESH: `oil_dds__event`.

Исторические ключи, точность Decimal, valid-time, удаления и правила выбора версий сохраняются в SQL. Новые технические load_dts/batch_id/received_at могут отличаться между движками; границы их сравнения заданы checks. Не заменяйте полную сверку одним COUNT или суммой.

Текущая раскладка: INITIAL — `dlh.silver.sat_daily_production`, REFRESH — `dlh.silver.sat_daily_production_next`. Загрузка и проверки выполняются задачей `oil_dds__sat_daily_production` в `oil_layered_migration`; зависимости и фазы заданы `oil_layered_plan.py`.
