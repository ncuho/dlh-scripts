# oil_dds__hub_field

Хранит бизнес-идентификатор месторождения и детерминированный hash key. Не содержит изменяемых описательных атрибутов.

Источник: `oil_dds.hub_field`. Целевая relation — физическая Iceberg таблица immutable release; для исходного view материализация заменяет runtime-view, бизнес-контракт сохраняется.

`ddl.sql` создаёт пустой объект через Cedrus/Trino. `initial.sql` — trino SELECT для C0. `refresh.sql` — spark SELECT для нового выпуска. `checks.sql` содержит блокирующие проверки с явной phase; исполняйте только проверки нужной фазы и both.

Зависимости INITIAL: исходный capture. REFRESH: `oil_dds__event`.

Исторические ключи, точность Decimal, valid-time, удаления и правила выбора версий сохраняются в SQL. Новые технические load_dts/batch_id/received_at могут отличаться между движками; границы их сравнения заданы checks. Не заменяйте полную сверку одним COUNT или суммой.
