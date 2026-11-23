# oil_stg__event_log

Принимает исходные JSON-события вместе с topic, partition, offset и ID партии. Сохраняет доставку до применения бизнес-логики.

Источник: `oil_stg.event_log`. Целевая relation — физическая Iceberg таблица immutable release; для исходного view материализация заменяет runtime-view, бизнес-контракт сохраняется.

`ddl.sql` создаёт пустой объект через Cedrus/Trino. `initial.sql` — trino SELECT для C0. `refresh.sql` — trino SELECT для нового выпуска. `checks.sql` содержит блокирующие проверки с явной phase; исполняйте только проверки нужной фазы и both.

Зависимости INITIAL: исходный capture. REFRESH: закреплённый INITIAL / raw.

Исторические ключи, точность Decimal, valid-time, удаления и правила выбора версий сохраняются в SQL. Новые технические load_dts/batch_id/received_at могут отличаться между движками; границы их сравнения заданы checks. Не заменяйте полную сверку одним COUNT или суммой.
