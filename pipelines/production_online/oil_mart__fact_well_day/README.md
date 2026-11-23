# oil_mart__fact_well_day

Факт зерна «скважина × производственная дата». Содержит тонны, статус и версии измерений well/field, действовавшие на дату.

Источник: `oil_mart.fact_well_day`. Целевая relation — физическая Iceberg таблица immutable release; для исходного view материализация заменяет runtime-view, бизнес-контракт сохраняется.

`ddl.sql` создаёт пустой объект через Cedrus/Trino. `initial.sql` — spark SELECT для C0. `refresh.sql` — spark SELECT для нового выпуска. `checks.sql` содержит блокирующие проверки с явной phase; исполняйте только проверки нужной фазы и both.

Зависимости INITIAL: `oil_dds__link_production_day_well`, `oil_dds__sat_daily_production`, `oil_mart__dim_field`, `oil_mart__dim_well`. REFRESH: `oil_dds__link_production_day_well`, `oil_dds__sat_daily_production`, `oil_mart__dim_field`, `oil_mart__dim_well`.

Исторические ключи, точность Decimal, valid-time, удаления и правила выбора версий сохраняются в SQL. Новые технические load_dts/batch_id/received_at могут отличаться между движками; границы их сравнения заданы checks. Не заменяйте полную сверку одним COUNT или суммой.
