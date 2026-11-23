# oil_stg__v_event_parsed

Разбирает envelope JSON в STG: тип, ID, версию, операцию, время и payload без изменения исходного документа.

Источник: `oil_stg.v_event_parsed`. Целевая relation — физическая Iceberg таблица immutable release; для исходного view материализация заменяет runtime-view, бизнес-контракт сохраняется.

`ddl.sql` создаёт пустой объект через Cedrus/Trino. `initial.sql` — spark SELECT для C0. `refresh.sql` — spark SELECT для нового выпуска. `checks.sql` содержит блокирующие проверки с явной phase; исполняйте только проверки нужной фазы и both.

Зависимости INITIAL: `oil_stg__event_log`. REFRESH: `oil_stg__event_log`.

Исторические ключи, точность Decimal, valid-time, удаления и правила выбора версий сохраняются в SQL. Новые технические load_dts/batch_id/received_at могут отличаться между движками; границы их сравнения заданы checks. Не заменяйте полную сверку одним COUNT или суммой.

Текущая раскладка: INITIAL — `dlh.bronze.v_event_parsed`, REFRESH — `dlh.bronze.v_event_parsed_next`. Загрузка и проверки выполняются задачей `oil_stg__v_event_parsed` в `oil_layered_migration`; зависимости и фазы заданы `oil_layered_plan.py`.
