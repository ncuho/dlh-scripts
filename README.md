# Нефтегазовый Lakehouse · Iceberg

Целевой контур отчётов добычи и простоев: GP snapshot + Kafka delta → Cedrus → immutable Iceberg release → Superset. LakeKeeper управляет Iceberg-каталогом и правами, CIAM — identities/ролями. [Паспорт данных](DATA-PASSPORT.md).

| Каталог | Ответственность |
|---|---|
| `ddl/wave1/` | 14 уже перенесённых таблиц первой цепочки и целевые схемы. |
| `ddl/ingestion/` | Независимый raw-приём Kafka второй цепочки. |
| `ddl/serving/` | Пустой источник второй панели до публикации release. |
| `ddl/releases/` | Отдельный SQL каждого baseline/canonical/Vault/mart объекта выпуска. |
| `queries/` | Read-only песочница и сверка первой цепочки. |
| `dags/` | `oil_wave2_release`: INITIAL, REFRESH, PUBLISH и повтор регистрации каталога. |
| `pipelines/` | Параметризованный SQL-план, CTAS runner, DQ и валидация manifest. |
| `migration/fence/` | Удержание и проверка общей границы Kafka/GP. |
| `publishing/` | Ограниченное переключение dataset, проверка запроса и rollback. |
| `grants/` | CIAM-backed роли LakeKeeper и grants raw/serving. |
| `access/` | TLS transport, SSH relay и независимый read-only verifier прав/snapshots. |
| `catalog/` | Регистрация фактически созданных объектов и обновление lineage после публикации. |
| `superset/` | Два datasets, восемь charts, две dashboards целевого контура. |
| `deploy/` | Сборка runtime в совместимый плоский каталог managed Airflow. |

Рядом с каждой сущностью — MD с назначением. SQL release содержит параметры/пример namespace; рабочие SQL генерируются pipeline из проверенного manifest. После разнесения source runtime собирается `deploy/package_runtime.py`, импорты действующего worker остаются прежними.

Первая цепочка опубликована в dashboard 5: 14 таблиц совпадают с GP на baseline. Вторая подготовлена: target connector PAUSED, raw и dataset 8 пусты. INITIAL создаёт неизменяемый baseline, 21 release table и READY после сверок; REFRESH использует тот же baseline и все записи offsets [start,end). После table-level SELECT reader в LakeKeeper отдельный PUBLISH переключает только dataset 8 и обновляет lineage.

INITIAL/PUBLISH/REFRESH с пустым новым окном проверены. Полный непустой REFRESH основного topic ещё не выполнен. Первая панель frozen и не обновляется автоматически при второй цепочке. Iceberg commits отдельных таблиц не являются распределённой транзакцией. Kafka RF=1; выбранный вычислитель — Cedrus. Ops patches после Helm/XaaS upgrade требуют проверки/reapply. Секреты и выгрузки метаданных вне Git.

[Исходный контур GP](https://github.com/ncuho/dwh-greenplum-scripts).
