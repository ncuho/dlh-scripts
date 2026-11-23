# oil_stg__batch_coalesce_map

Связывает исходные партии с объединённой партией и сохраняет число исходных доставок.

Источник: `oil_stg.batch_coalesce_map`. Целевая relation — физическая Iceberg таблица immutable release; для исходного view материализация заменяет runtime-view, бизнес-контракт сохраняется.

`ddl.sql` создаёт пустой объект через Cedrus/Trino. `initial.sql` — trino SELECT для C0. `refresh.sql` — trino SELECT для нового выпуска. `checks.sql` содержит блокирующие проверки с явной phase; исполняйте только проверки нужной фазы и both.

Зависимости INITIAL: исходный capture. REFRESH: закреплённый INITIAL / raw.

Исторические ключи, точность Decimal, valid-time, удаления и правила выбора версий сохраняются в SQL. Новые технические load_dts/batch_id/received_at могут отличаться между движками; границы их сравнения заданы checks. Не заменяйте полную сверку одним COUNT или суммой.

Текущая раскладка: INITIAL — `dlh.bronze.batch_coalesce_map`, REFRESH — `dlh.bronze.batch_coalesce_map_next`. Загрузка и проверки выполняются задачей `oil_stg__batch_coalesce_map` в `oil_layered_migration`; зависимости и фазы заданы `oil_layered_plan.py`.
