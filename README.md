# Lakehouse на Iceberg

## Текущий сквозной маршрут

Предметные слои: Bronze — Stage и landing Kafka, Silver — Data Vault, Gold — размерная модель и отчёт добычи. INITIAL сохраняет исходные имена таблиц; следующий кандидат использует `_next`. Данные и код исходного GP не удаляются.

DAG `oil_layered_migration` управляет всеми 20 объектами, проверяет родителей перед дочерними таблицами, выполняет 60 INITIAL / 53 REFRESH проверок и компенсирует подтверждённые записи кандидата при сбое. Неизвестный исход и чужие изменения требуют сверки; публикация Superset остаётся отдельной приёмкой после READY. Код runtime и план — [pipelines/oil_layered_migration](pipelines/oil_layered_migration), DAG — [dags/oil_layered_migration](dags/oil_layered_migration).

Текущий online-маршрут: [oil_source_snapshot](dags/oil_source_snapshot) → [oil_layered_migration](dags/oil_layered_migration). Технический вход: [__source_snapshots](ddl/layers/bronze/__source_snapshots).

Догон читает raw с перекрытием по offsets и детерминированной дедупликацией. Повторное чтение не должно создавать дубли; конфликт содержимого блокирует выпуск. Сохранённые альтернативные runtime ниже не являются командой текущего ручного маршрута.

[Архитектура со слоями и моделями](architecture/README.md). Комментарии SQL/Python поясняют назначение, исходную логику GP и целевые отличия. Физические DDL текущего маршрута находятся в [ddl/layers](ddl/layers).


## Текущее состояние

Целевой контур сброшен к началу ручного прохода: предметные схемы и целевой connector отсутствуют. Исходные GP, Kafka topic, offsets и Superset dashboards 3 и 4 сохранены. Код установлен; миграционные DAG на паузе. Оператор начинает с создания целей и запускает два DAG вручную. Подготовленный код не означает, что бизнес-перенос уже выполнен.

## Данные и три слоя Greenplum

Один поток `oil.events.v1` содержит четыре типа событий: месторождение, скважина, суточная добыча, простой. Исходный набор: 5 месторождений, 40 скважин, 365 дат 2025 года. Добыча хранится в decimal, время — UTC.

- **Stage / oil_stg:** исходный JSON, координаты Kafka, партии, checkpoints, rejects и аудит объединения партий. Приём сообщения ещё не означает его применение в DDS.
- **DDS / oil_dds:** event ledger, hubs ключей, links отношений, satellites версий. Используются паттерны DV2 и аудит загрузки; полная enterprise-реализация всех практик Data Vault 2.0 не заявляется.
- **Marts / oil_mart:** исторические SCD2-измерения, факт скважино-дня, факт интервала простоя, дневные агрегаты и serving views. Это связанные размерные модели с общими измерениями; не произвольное объединение таблиц и не строгая отдельная snowflake-модель.

Продуктивность = SUM(oil_tonnes) / NULLIF(SUM(producing_well_days), 0). Это тонны на активный скважино-день, а не тонны на уникальную скважину за весь период. Факт присоединяется к действовавшей версии измерения, а не к текущему названию/статусу.

## Две цепочки и общие зависимости

```text
generator → Kafka oil.events.v1 → GP sink
  → Stage event_log / batch_control / checkpoint
  → apply_batch → DDS event + hubs / links / satellites
  → refresh_production → dim_field / dim_well
  → fact_well_day → agg_field_day → v_field_production
  → Superset dataset 4 / dashboard 3

та же история + downtime
  → hub_downtime / link_downtime_well / sat_downtime
  → refresh_downtime (после refresh_production)
  → dim_downtime_reason / fact_downtime_interval
  → agg_field_downtime_day + agg_field_day → v_field_performance
  → Superset dataset 5 / dashboard 4
```

Это один источник и два бизнес-отчёта. Stage, apply_batch, производственные измерения/факты и source DAG `oil_gp_release` общие. Изменение добычи или скважины может изменить оба исходных отчёта; изменение простоя — ветку эффективности. Source DAG применяет партии и последовательно вызывает обе функции расчёта.

Целевой Spark SQL переиспользуется как код. Физические baseline/releases двух волн по умолчанию отдельные; публикация одной не переключает другую. Переиспользование общих физических target-измерений требует отдельного согласованного snapshot-контракта.

## Целевая архитектура со Spark

```text
GP REPEATABLE READ READ ONLY → native reader → Cedrus → Iceberg historical input
SEALED input → Cedrus / Spark → Bronze / Silver / Gold INITIAL
Kafka → отдельный Iceberg sink → Iceberg raw JSON + offsets
pinned INITIAL + накопительная дельта → Spark → новый Iceberg release
release → Cedrus → отдельный Superset dataset / dashboard
Airflow: порядок и logs; LakeKeeper + CIAM: доступ; Catalog: описания и lineage
```

Cedrus извлекает историю и выполняет межконтурные сверки. Spark выполняет версии, SCD2, факты и агрегаты. JupyterHub нужен для явного просмотра/исполнения кода; Airflow запускает отдельную Spark-сессию через выделенный MCP/Spark Connect. Исторический Cedrus-only код сохранён, но выбранный новый маршрут бизнес-расчётов — Spark.

Текущий layered-маршрут переносит 6 Stage, 9 Vault и 5 Marts/serving объектов, включая аудиторские поля и сохранённые ключи. GP views материализуются в Iceberg-таблицы. INITIAL копирует 14 объектов через Cedrus и рассчитывает 6 через Spark; REFRESH использует 5 Cedrus и 15 Spark операций. GP DISTRIBUTED BY, индексы и PL/pgSQL runtime не копируются в Iceberg буквально: их заменяют файловая организация, отдельные compute engines, DAG и DQ. Прежние альтернативные реализации с проекцией `sat_production_day` сохранены как код, но не исполняются текущим DAG.

## Граница, изменения и публикация

1. Оператор создаёт Bronze/Silver/Gold, выдаёт сервису MODIFY на новый UUID raw и создаёт отдельный Iceberg sink. GP sink не меняется.
2. Producer отправляет события до начала C0 и продолжает работать весь перенос. Исходный `oil_gp_release` применяет события и обновляет оба исходных отчёта.
3. Оператор запускает `oil_source_snapshot` с `{"mode":"initial"}`. Один native GP connection использует REPEATABLE READ READ ONLY для всех 20 объектов. READY backlog остаётся за C0/C1: задача выбирает непрерывный APPLIED-префикс и сохраняет отдельную границу приёма. Stage проецируется на применённую историю, DDS/Marts читаются из того же MVCC snapshot. Пустую очередь ждать не требуется. Плановых пауз источника и блокировок записи нет.
4. Извлечённые значения записываются через Cedrus в `bronze.__source_snapshots`. Проверяются SHA-256 блоков, число строк и координаты. SEALED закрепляет версию Iceberg и offsets C0. После этого исходная read-only транзакция закрывается.
5. Оператор копирует `next_dag_config` в `oil_layered_migration`. INITIAL читает сохранённый экспорт, выполняет 14 Cedrus и 6 Spark операций и 60 проверок. Только READY допускает публикацию отдельного целевого отчёта.
6. Для C1 оператор запускает `oil_source_snapshot` с `{"mode":"refresh"}`. Затем передаёт `next_dag_config` и `overlap_offsets=10` в миграционный DAG.
7. REFRESH строит `_next` из закреплённого INITIAL и полного raw-диапазона C0–C1 с перекрытием перед C0. Stage дедуплицирует доставки, DDS — event_id, entity_version выбирает бизнес-версию. Конфликт данных запрещает выпуск.
8. Пять Cedrus и пятнадцать Spark операций, 53 проверки и отдельное переключение целевого BI. Сверки используют сохранённый экспорт C1, а не уже изменившийся live GP. События после C1 остаются в raw для последующей обработки.

Установленный GP connector Cedrus поддерживает READ COMMITTED и не наследует native snapshot. Поэтому единый снимок читает psycopg2; Cedrus записывает транспорт и переносит типизированные значения из закреплённого Iceberg input. Spark считает бизнес-логику. Общей ACID-транзакции GP и Iceberg нет. Ошибка блокирует публикацию и запускает компенсацию целевого кандидата; исходный GP не откатывается.

Kafka доставляет события приложения, это не WAL CDC Greenplum. Изменения вне событийного контракта требуют отдельного механизма CDC. PXF/external tables не участвуют в этой цепочке.

## Доступы и ответственность сервисов

| Сервис | За что отвечает |
|---|---|
| GP | Исходные приём, история, функции и транзакционные расчёты |
| Kafka / Connect | Producer → topic; отдельные GP и Iceberg consumers и offset stores |
| Airflow | Порядок выполнения, run IDs и журнал; не заменяет общую транзакцию Iceberg |
| Spark | Целевые преобразования и материализация бизнес-слоёв |
| Cedrus | Чтение GP/Iceberg, историческая загрузка, сверка и SQL для BI |
| LakeKeeper | Iceberg REST catalog и permissions на реальные table UUID |
| CIAM | Идентичности людей/сервисов и роли |
| S3 / Iceberg | Файлы, metadata, snapshots и отдельно подтверждённые releases |
| Superset | Dataset, charts, dashboard и собственный BI RBAC |
| Data Catalog | Реальные сущности, описания, owners и сквозной lineage |
| GitHub | Версионируемый код; не хранилище secrets/runtime evidence |

Инженеру нужен create/write scope; sink получает MODIFY raw; reader — SELECT serving. Старый GRANT GP не превращается автоматически в LakeKeeper assignment. При новой таблице возникает новый UUID и проверяется новое разрешение. В BI отдельно проверяются пользовательская роль и identity SQL-соединения: end-user passthrough не подразумевается.

## Организация кода

Каждая сущность — отдельная папка с кодом и README. Не меняйте технические ID connector/consumer group без переноса offsets и прав. Credentials и runtime manifests хранятся вне Git.

## Полный указатель сущностей

Ссылки ведут в папки с автоматически открывающимся описанием; служебные реализации не обозначены как перенесённые бизнес-таблицы.

| Сущность | Назначение |
|---|---|
| [access/transport/configure_fence_relay](access/transport/configure_fence_relay/) | Настраивает ограниченный SSH relay от Airflow к Kafka fence с forced command и закреплёнными host keys. |
| [access/transport/configure_runtime_tls](access/transport/configure_runtime_tls/) | Настраивает HTTPS+Bearer gateway для вызовов runtime к SQL MCP, каталогу и проверке serving-доступов. |
| [access/verifier/configure_lakekeeper_verifier](access/verifier/configure_lakekeeper_verifier/) | Разворачивает verifier с TLS, авторизацией и ограничением сетевого доступа. Секреты хранятся в Kubernetes Secret. |
| [access/verifier/lakekeeper_verifier](access/verifier/lakekeeper_verifier/) | Read-only сервис проверки актуальных snapshots, table-level grants и отсутствия избыточного родительского доступа перед публикацией. |
| [catalog/catalog_launcher_overlay](catalog/catalog_launcher_overlay/) | Расширяет MCP каталога узкими операциями удаления BI-lineage и обновления описаний разрешённых BI-объектов. |
| [catalog/deploy_catalog_overlay](catalog/deploy_catalog_overlay/) | Подключает versioned overlay к MCP через ConfigMap. После Helm/XaaS reconcile требуется сверка и при необходимости reapply. |
| [catalog/runtime_catalog](catalog/runtime_catalog/) | Регистрирует только фактически созданный и сверенный release. После подтверждённой публикации обновляет lineage к графикам и описание текущего источника. |
| [dags/oil_wave2_release](dags/oil_wave2_release/) | Ручной DAG подготовки, сверки и публикации отчётности нефтедобычи. Airflow: `http://10.0.2.13:8080`. Рабочий код: oil_wave2_release.py. |
| [ddl/ingestion/event_log_wave2](ddl/ingestion/event_log_wave2/) | Отдельная raw Iceberg-таблица Kafka sink. Хранит Kafka-координаты и полный JSON, не изменяя ранее принятый baseline первой цепочки. |
| [ddl/releases/agg_field_day](ddl/releases/agg_field_day/) | Дневная витрина зерна «месторождение × дата»: тонны, активные скважино-дни и тонны на активный скважино-день. |
| [ddl/releases/agg_field_downtime_day](ddl/releases/agg_field_downtime_day/) | Дневная сумма часов допустимых непересекающихся простоев по месторождению. |
| [ddl/releases/baseline_event](ddl/releases/baseline_event/) | Неизменяемая копия полного реестра GP events на INITIAL cut. Один и тот же snapshot используется последующими REFRESH. |
| [ddl/releases/canonical_event](ddl/releases/canonical_event/) | Убирает дословные повторы документов и разбирает envelope. Конфликтующие ID/версии не замалчиваются: их блокируют DQ gates. |
| [ddl/releases/canonical_input](ddl/releases/canonical_input/) | Объединяет исходный baseline и raw-события из всех полуоткрытых диапазонов offsets [start,end), зафиксированных в manifest. |
| [ddl/releases/dim_downtime_reason](ddl/releases/dim_downtime_reason/) | Справочник причин: EQUIPMENT, POWER, PLANNED. Нормализует группировку простоев. |
| [ddl/releases/dim_field](ddl/releases/dim_field/) | Историческое измерение месторождения. Интервал действия версии — [valid_from, valid_to); удаление сохраняется отдельным признаком. |
| [ddl/releases/dim_well](ddl/releases/dim_well/) | Историческое измерение скважины, её принадлежности месторождению и статуса. Версия выбирается на производственную дату. |
| [ddl/releases/fact_downtime_interval](ddl/releases/fact_downtime_interval/) | Факт зерна «интервал простоя». Содержит скважину, месторождение, начало/конец, причину и длительность в часах. |
| [ddl/releases/fact_well_day](ddl/releases/fact_well_day/) | Факт зерна «скважина × производственная дата». Содержит тонны, статус и версии измерений well/field, действовавшие на дату. |
| [ddl/releases/field_performance](ddl/releases/field_performance/) | Физическая serving-таблица второй цепочки. Объединяет добычу и простои по дате/месторождению и используется целевым BI. |
| [ddl/releases/hub_downtime](ddl/releases/hub_downtime/) | Хранит бизнес-идентификатор интервала простоя и детерминированный hash key. Не содержит изменяемых описательных атрибутов. |
| [ddl/releases/hub_field](ddl/releases/hub_field/) | Хранит бизнес-идентификатор месторождения и детерминированный hash key. Не содержит изменяемых описательных атрибутов. |
| [ddl/releases/hub_production_day](ddl/releases/hub_production_day/) | Хранит бизнес-идентификатор скважино-дня добычи и детерминированный hash key. Не содержит изменяемых описательных атрибутов. |
| [ddl/releases/hub_well](ddl/releases/hub_well/) | Хранит бизнес-идентификатор скважины и детерминированный hash key. Не содержит изменяемых описательных атрибутов. |
| [ddl/releases/link_downtime_well](ddl/releases/link_downtime_well/) | Хранит историческую связь интервала простоя со скважиной по техническим ключам. Изменяемые атрибуты находятся в satellites. |
| [ddl/releases/link_production_day_well](ddl/releases/link_production_day_well/) | Хранит историческую связь скважино-дня со скважиной по техническим ключам. Изменяемые атрибуты находятся в satellites. |
| [ddl/releases/link_well_field](ddl/releases/link_well_field/) | Хранит историческую связь скважины с месторождением по техническим ключам. Изменяемые атрибуты находятся в satellites. |
| [ddl/releases/sat_downtime](ddl/releases/sat_downtime/) | Сохраняет версии атрибутов интервала простоя, event_id, entity_version и операцию U/D. GP хранит типизированный payload и audit-поля; release DLH использует логическую проекцию с исходным document. |
| [ddl/releases/sat_field](ddl/releases/sat_field/) | Сохраняет версии атрибутов месторождения, event_id, entity_version и операцию U/D. GP хранит типизированный payload и audit-поля; release DLH использует логическую проекцию с исходным document. |
| [ddl/releases/sat_production_day](ddl/releases/sat_production_day/) | Сохраняет версии атрибутов дневной добычи, event_id, entity_version и операцию U/D. GP хранит типизированный payload и audit-поля; release DLH использует логическую проекцию с исходным document. |
| [ddl/releases/sat_well](ddl/releases/sat_well/) | Сохраняет версии атрибутов скважины, event_id, entity_version и операцию U/D. GP хранит типизированный payload и audit-поля; release DLH использует логическую проекцию с исходным document. |
| [ddl/serving/field_performance](ddl/serving/field_performance/) | Физическая serving-таблица второй цепочки. Объединяет добычу и простои по дате/месторождению и используется целевым BI. |
| [ddl/serving/oil_demo_template](ddl/serving/oil_demo_template/) | Namespace пустой serving-таблицы, используемой до первого подтверждённого выпуска второй цепочки. |
| [ddl/wave1/schemas/oil_dds](ddl/wave1/schemas/oil_dds/) | Схема интеграции и истории бизнес-сущностей с hubs, links и satellites. |
| [ddl/wave1/schemas/oil_mart](ddl/wave1/schemas/oil_mart/) | Схема измерений, фактов и serving-агрегатов управленческой отчётности. |
| [ddl/wave1/schemas/oil_stg](ddl/wave1/schemas/oil_stg/) | Схема приёма неизменённых событий и технического контроля загрузки. |
| [ddl/wave1/tables/oil_dds/event](ddl/wave1/tables/oil_dds/event/) | Реестр принятых событий DDS. event_id определяет повтор, entity_version задаёт порядок изменений, event_document позволяет сверить историю. |
| [ddl/wave1/tables/oil_dds/hub_field](ddl/wave1/tables/oil_dds/hub_field/) | Хранит бизнес-идентификатор месторождения и детерминированный hash key. Не содержит изменяемых описательных атрибутов. |
| [ddl/wave1/tables/oil_dds/hub_production_day](ddl/wave1/tables/oil_dds/hub_production_day/) | Хранит бизнес-идентификатор скважино-дня добычи и детерминированный hash key. Не содержит изменяемых описательных атрибутов. |
| [ddl/wave1/tables/oil_dds/hub_well](ddl/wave1/tables/oil_dds/hub_well/) | Хранит бизнес-идентификатор скважины и детерминированный hash key. Не содержит изменяемых описательных атрибутов. |
| [ddl/wave1/tables/oil_dds/link_production_day_well](ddl/wave1/tables/oil_dds/link_production_day_well/) | Хранит историческую связь скважино-дня со скважиной по техническим ключам. Изменяемые атрибуты находятся в satellites. |
| [ddl/wave1/tables/oil_dds/link_well_field](ddl/wave1/tables/oil_dds/link_well_field/) | Хранит историческую связь скважины с месторождением по техническим ключам. Изменяемые атрибуты находятся в satellites. |
| [ddl/wave1/tables/oil_dds/sat_daily_production](ddl/wave1/tables/oil_dds/sat_daily_production/) | Сохраняет версии атрибутов дневной добычи, event_id, entity_version и операцию U/D. GP хранит типизированный payload и audit-поля; release DLH использует логическую проекцию с исходным document. |
| [ddl/wave1/tables/oil_dds/sat_field](ddl/wave1/tables/oil_dds/sat_field/) | Сохраняет версии атрибутов месторождения, event_id, entity_version и операцию U/D. GP хранит типизированный payload и audit-поля; release DLH использует логическую проекцию с исходным document. |
| [ddl/wave1/tables/oil_dds/sat_well](ddl/wave1/tables/oil_dds/sat_well/) | Сохраняет версии атрибутов скважины, event_id, entity_version и операцию U/D. GP хранит типизированный payload и audit-поля; release DLH использует логическую проекцию с исходным document. |
| [ddl/wave1/tables/oil_mart/agg_field_day](ddl/wave1/tables/oil_mart/agg_field_day/) | Дневная витрина зерна «месторождение × дата»: тонны, активные скважино-дни и тонны на активный скважино-день. |
| [ddl/wave1/tables/oil_mart/dim_field](ddl/wave1/tables/oil_mart/dim_field/) | Историческое измерение месторождения. Интервал действия версии — [valid_from, valid_to); удаление сохраняется отдельным признаком. |
| [ddl/wave1/tables/oil_mart/dim_well](ddl/wave1/tables/oil_mart/dim_well/) | Историческое измерение скважины, её принадлежности месторождению и статуса. Версия выбирается на производственную дату. |
| [ddl/wave1/tables/oil_mart/fact_well_day](ddl/wave1/tables/oil_mart/fact_well_day/) | Факт зерна «скважина × производственная дата». Содержит тонны, статус и версии измерений well/field, действовавшие на дату. |
| [ddl/wave1/tables/oil_stg/event_log](ddl/wave1/tables/oil_stg/event_log/) | Принимает исходные JSON-события вместе с topic, partition, offset и ID партии. Сохраняет доставку до применения бизнес-логики. |
| [deploy/package_runtime](deploy/package_runtime/) | Собирает categorized source в плоский каталог managed Airflow, сохраняя действующие имена импортов. Публикацию и перезапуск сервисов не выполняет. |
| [grants/lakekeeper/grant_ingestion](grants/lakekeeper/grant_ingestion/) | Назначает отдельной CIAM service identity право modify только на выделенную raw-таблицу Iceberg. |
| [grants/lakekeeper/grant_serving](grants/lakekeeper/grant_serving/) | Admin-утилита назначения reader SELECT на две serving-таблицы конкретного готового release; проверяет snapshots. Основной операторский способ — LakeKeeper UI. |
| [grants/lakekeeper/lakekeeper_roles](grants/lakekeeper/lakekeeper_roles/) | Сопоставляет CIAM-backed reader/engineer роли с разрешениями LakeKeeper через штатный Management API. |
| [migration/fence/fence_controller](migration/fence/fence_controller/) | Адаптер ограниченного SSH-вызова acquire/status/release удалённого контроллера fence. |
| [migration/fence/fence_probe_socket](migration/fence/fence_probe_socket/) | Проверяет действительность fence через локальный канал управляющей задачи; используется runner перед операциями. |
| [migration/fence/source_fence](migration/fence/source_fence/) | Прежний вариант с producer fence. Исторический код; текущий native snapshot не блокирует producer, GP sink или source DAG. |
| [pipelines/bounded_delta](pipelines/bounded_delta/) | Проверяет структуру manifest, идентификаторы таблиц, snapshots и offset-диапазоны; атомарно сохраняет evidence. |
| [pipelines/cedrus_release_runner](pipelines/cedrus_release_runner/) | Создаёт 21 release-таблицу в порядке зависимостей через Cedrus. Проверяет snapshots, непрерывность offsets и результаты; сохраняет evidence, поддерживает ограниченное восстановление. |
| [pipelines/dq_sql](pipelines/dq_sql/) | Генерирует read-only проверки конфликтов, ссылочной полноты, SCD, статусов, интервалов, истории событий и двусторонние EXCEPT ALL с GP. |
| [pipelines/sql_plan](pipelines/sql_plan/) | Генерирует параметризованный CTAS для 11 Vault-проекций и 8 marts. Допускает только проверенное имя нового release namespace. |
| [publishing/publish_dataset](publishing/publish_dataset/) | Переключает только разрешённый Superset dataset при совпадении прежней привязки. Проверяет новый запрос и выполняет rollback при ошибке. |
| [publishing/superset_auth](publishing/superset_auth/) | Получает короткий JWT ограниченного publisher по HTTPS, держит его во временном файле и удаляет после операции. |
| [queries/sandbox/production_report](queries/sandbox/production_report/) | Read-only запрос к опубликованному baseline первой цепочки через Cedrus. Сравнение с GP корректно только на одной границе. |
| [queries/validation/03_wave1_reconcile](queries/validation/03_wave1_reconcile/) | Read-only SQL-проверки качества, полноты и согласованности результатов двух контуров. |
| [superset/charts/performance_by_field](superset/charts/performance_by_field/) | График «Показатели по месторождениям». Использует dataset 8; параметры метрик, агрегации, временной гранулярности и отображения заданы в params. |
| [superset/charts/performance_downtime_total](superset/charts/performance_downtime_total/) | График «Часы простоев». Использует dataset 8; параметры метрик, агрегации, временной гранулярности и отображения заданы в params. |
| [superset/charts/performance_oil_total](superset/charts/performance_oil_total/) | График «Добыча, т». Использует dataset 8; параметры метрик, агрегации, временной гранулярности и отображения заданы в params. |
| [superset/charts/performance_trend](superset/charts/performance_trend/) | График «Динамика простоев по месторождениям». Использует dataset 8; параметры метрик, агрегации, временной гранулярности и отображения заданы в params. |
| [superset/charts/production_active_well_days](superset/charts/production_active_well_days/) | График «Активные скважино-дни». Использует dataset 6; параметры метрик, агрегации, временной гранулярности и отображения заданы в params. |
| [superset/charts/production_by_field](superset/charts/production_by_field/) | График «Показатели по месторождениям». Использует dataset 6; параметры метрик, агрегации, временной гранулярности и отображения заданы в params. |
| [superset/charts/production_oil_total](superset/charts/production_oil_total/) | График «Добыча, т». Использует dataset 6; параметры метрик, агрегации, временной гранулярности и отображения заданы в params. |
| [superset/charts/production_trend](superset/charts/production_trend/) | График «Динамика добычи по месторождениям». Использует dataset 6; параметры метрик, агрегации, временной гранулярности и отображения заданы в params. |
| [superset/dashboards/performance](superset/dashboards/performance/) | Панель «Эффективность добычи и простои · Lakehouse». Содержит сохранённые графики, layout и фильтры региона/месторождения. Источник определяется соответствующим dataset; публикация release выполняется отдельно. |
| [superset/dashboards/production](superset/dashboards/production/) | Панель «Добыча по месторождениям · Lakehouse». Содержит сохранённые графики, layout и фильтры региона/месторождения. Источник определяется соответствующим dataset; публикация release выполняется отдельно. |
| [superset/datasets/performance](superset/datasets/performance/) | Dataset 8 читает oil_demo_template.field_performance. Определяет временное поле, колонки и доступные метрики для графиков. |
| [superset/datasets/production](superset/datasets/production/) | Dataset 6 читает oil_mart.agg_field_day. Определяет временное поле, колонки и доступные метрики для графиков. |

Смежный репозиторий: [Greenplum](https://github.com/ncuho/dwh-greenplum-scripts).



## Spark

- [spark/spark_plan](spark/spark_plan/)
- [spark/sql/agg_field_day](spark/sql/agg_field_day/)
- [spark/sql/agg_field_downtime_day](spark/sql/agg_field_downtime_day/)
- [spark/sql/dim_downtime_reason](spark/sql/dim_downtime_reason/)
- [spark/sql/dim_field](spark/sql/dim_field/)
- [spark/sql/dim_well](spark/sql/dim_well/)
- [spark/sql/fact_downtime_interval](spark/sql/fact_downtime_interval/)
- [spark/sql/fact_well_day](spark/sql/fact_well_day/)
- [spark/sql/field_performance](spark/sql/field_performance/)
- [spark/sql/hub_downtime](spark/sql/hub_downtime/)
- [spark/sql/hub_field](spark/sql/hub_field/)
- [spark/sql/hub_production_day](spark/sql/hub_production_day/)
- [spark/sql/hub_well](spark/sql/hub_well/)
- [spark/sql/link_downtime_well](spark/sql/link_downtime_well/)
- [spark/sql/link_production_day_well](spark/sql/link_production_day_well/)
- [spark/sql/link_well_field](spark/sql/link_well_field/)
- [spark/sql/sat_downtime](spark/sql/sat_downtime/)
- [spark/sql/sat_field](spark/sql/sat_field/)
- [spark/sql/sat_production_day](spark/sql/sat_production_day/)
- [spark/sql/sat_well](spark/sql/sat_well/)

## Online-перенос производственной цепочки

Историческая загрузка читает сохранённый SEALED экспорт одного native GP snapshot. GP ingestion, расчёты и producer продолжают работу. REFRESH использует полный C0–C1 с перекрытием; служебные таблицы внутри GP не создаются.

Полный исходный/целевой SQL прокомментирован. В целевом репозитории `pipelines/production_online` содержит 20 отдельных папок: DDL, INITIAL, REFRESH, проверки и назначение каждого объекта. Это принятый бизнес-контракт первой цепочки. Остальные SQL относятся к альтернативным проекциям и следующей волне; не запускайте их поверх этого release. В Git не записываются runtime receipts, snapshots и секреты.

LakeKeeper управляет таблицами и доступом через CIAM identities, Spark выполняет бизнес-проекции, Cedrus переносит историю и обслуживает SQL, Airflow запускает принятую обработку. Kafka Connect сохраняет JSON и координаты, обязательный received_at фиксируется SMT. Kafbat UI наблюдает topics/messages/groups/Connect через приватный SSH-туннель.
