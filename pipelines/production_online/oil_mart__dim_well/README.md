# oil_mart__dim_well

Историческое измерение скважины, её принадлежности месторождению и статуса. Версия выбирается на производственную дату.

Источник: `oil_mart.dim_well`. Целевая relation — физическая Iceberg таблица immutable release; для исходного view материализация заменяет runtime-view, бизнес-контракт сохраняется.

`ddl.sql` создаёт пустой объект через Cedrus/Trino. `initial.sql` — spark SELECT для C0. `refresh.sql` — spark SELECT для нового выпуска. `checks.sql` содержит блокирующие проверки с явной phase; исполняйте только проверки нужной фазы и both.

Зависимости INITIAL: `oil_dds__link_well_field`, `oil_dds__sat_well`. REFRESH: `oil_dds__link_well_field`, `oil_dds__sat_well`.

Исторические ключи, точность Decimal, valid-time, удаления и правила выбора версий сохраняются в SQL. Новые технические load_dts/batch_id/received_at могут отличаться между движками; границы их сравнения заданы checks. Не заменяйте полную сверку одним COUNT или суммой.

Текущая раскладка: INITIAL — `dlh.gold.dim_well`, REFRESH — `dlh.gold.dim_well_next`. Загрузка и проверки выполняются задачей `oil_mart__dim_well` в `oil_layered_migration`; зависимости и фазы заданы `oil_layered_plan.py`.
