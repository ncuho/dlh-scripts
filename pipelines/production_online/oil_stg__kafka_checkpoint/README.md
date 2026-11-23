# oil_stg__kafka_checkpoint

Хранит следующий ожидаемый offset каждой партиции. Обновляется в одной транзакции с записью события в GP.

Источник: `oil_stg.kafka_checkpoint`. Целевая relation — физическая Iceberg таблица immutable release; для исходного view материализация заменяет runtime-view, бизнес-контракт сохраняется.

`ddl.sql` создаёт пустой объект через Cedrus/Trino. `initial.sql` — trino SELECT для C0. `refresh.sql` — trino SELECT для нового выпуска. `checks.sql` содержит блокирующие проверки с явной phase; исполняйте только проверки нужной фазы и both.

Зависимости INITIAL: исходный capture. REFRESH: закреплённый INITIAL / raw.

Исторические ключи, точность Decimal, valid-time, удаления и правила выбора версий сохраняются в SQL. Новые технические load_dts/batch_id/received_at могут отличаться между движками; границы их сравнения заданы checks. Не заменяйте полную сверку одним COUNT или суммой.

Текущая раскладка: INITIAL — `dlh.bronze.kafka_checkpoint`, REFRESH — `dlh.bronze.kafka_checkpoint_next`. Загрузка и проверки выполняются задачей `oil_stg__kafka_checkpoint` в `oil_layered_migration`; зависимости и фазы заданы `oil_layered_plan.py`.
