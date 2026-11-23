# oil_mart__agg_field_day

Дневная витрина зерна «месторождение × дата»: тонны, активные скважино-дни и тонны на активный скважино-день.

Источник: `oil_mart.agg_field_day`. Целевая relation — физическая Iceberg таблица immutable release; для исходного view материализация заменяет runtime-view, бизнес-контракт сохраняется.

`ddl.sql` создаёт пустой объект через Cedrus/Trino. `initial.sql` — spark SELECT для C0. `refresh.sql` — spark SELECT для нового выпуска. `checks.sql` содержит блокирующие проверки с явной phase; исполняйте только проверки нужной фазы и both.

Зависимости INITIAL: `oil_mart__dim_field`, `oil_mart__fact_well_day`. REFRESH: `oil_mart__dim_field`, `oil_mart__fact_well_day`.

Исторические ключи, точность Decimal, valid-time, удаления и правила выбора версий сохраняются в SQL. Новые технические load_dts/batch_id/received_at могут отличаться между движками; границы их сравнения заданы checks. Не заменяйте полную сверку одним COUNT или суммой.

Текущая раскладка: INITIAL — `dlh.gold.agg_field_day`, REFRESH — `dlh.gold.agg_field_day_next`. Загрузка и проверки выполняются задачей `oil_mart__agg_field_day` в `oil_layered_migration`; зависимости и фазы заданы `oil_layered_plan.py`.
