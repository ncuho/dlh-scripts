# Нефтегазовый Lakehouse

Целевой контур миграции Greenplum → Iceberg. Первая цепочка добычи уже работает: 14 таблиц и dashboard 5. Вторая цепочка эффективности/простоев подготовлена: connector PAUSED, raw и dashboard 6 пустые до запуска демонстрации.

```text
GP ── Cedrus initial snapshot ──→ immutable baseline ─┐
Kafka → отдельный Iceberg Connect → raw delta ───────┤
                                                   ↓ Airflow oil_wave2_release
                          canonical events → DDS projections → marts → Superset
                                      Iceberg на S3 + LakeKeeper / CIAM
```

## Код и сущности

| Файл или каталог | Назначение |
|---|---|
| `sql/wave1/` | Реальные CTAS и расчёты первой волны; отдельный SQL на каждый объект. Target view не используется. |
| `sql/ingestion/event_log_wave2.sql` | Изолированный raw-приём Kafka для второй цепочки; не меняет baseline первой. |
| `sql/template/` | Пустая serving-таблица для подготовленного второго dashboard. |
| `sql/release/baseline_event.sql` | Immutable полный реестр GP events для INITIAL. |
| `sql/release/canonical_input.sql` | Baseline плюс raw records из полуоткрытых offset-окон по manifest. |
| `sql/release/canonical_event.sql` | Deduplication одинаковых документов и разбор envelope. |
| `sql/release/hub_*.sql`, `link_*.sql`, `sat_*.sql` | 11 логических Vault-проекций истории событий. |
| `sql/release/dim_*.sql` | Исторические field/well и причины простоев. |
| `sql/release/fact_well_day.sql` | Добыча скважины за день с корректной исторической версией измерений. |
| `sql/release/fact_downtime_interval.sql` | Последняя действующая версия интервала и его длительность. |
| `sql/release/agg_field_day.sql`, `agg_field_downtime_day.sql` | Дневные суммы по месторождению. |
| `sql/release/field_performance.sql` | Физическая serving-таблица добычи с часами простоев. |
| `runtime/airflow_oil_wave2.py` | Ручной DAG `oil_wave2_release`: INITIAL, REFRESH, PUBLISH и повтор каталожного шага. |
| `runtime/source_fence.py`, `fence_controller.py`, `fence_probe_socket.py` | Lease Kafka, pause GP sink, удержание GP advisory lock и проверка живой границы. |
| `runtime/bounded_delta.py` | Валидация manifest и атомарная запись evidence; Spark-альтернатива удалена. |
| `runtime/sql_plan.py`, `cedrus_release_runner.py` | Генерация parameterized SQL и создание 21 release table через Cedrus. |
| `runtime/dq_sql.py`, `quality/` | DQ, полная история событий и двусторонние EXCEPT ALL с GP на общем cut. |
| `runtime/publish_dataset.py`, `superset_auth.py` | Ограниченное переключение dataset 8, проверка запроса и rollback; короткий JWT. |
| `runtime/grant_serving.py` | Admin-утилита SELECT на две serving tables; штатный основной способ — LakeKeeper UI. |
| `runtime/runtime_catalog.py` | Регистрация реально созданного release и обновление lineage после публикации. |
| `ops/` | Роли LakeKeeper, TLS gateway, fence relay, read-only verifier и необходимый MCP overlay каталога. |
| `superset/` | Отдельный декларативный JSON для каждого dataset, chart и dashboard целевого контура. |

SQL в `sql/release/` — читаемое представление запросов runtime. `oil_r_example` и `__...__` заменяются параметрами manifest; эти файлы нельзя исполнять подряд вслепую. Рабочий DAG генерирует конкретные SQL через `sql_plan.py` и runner, обеспечивая порядок зависимостей.

## Запуск второй цепочки

В Airflow → `oil_wave2_release` → Trigger DAG with config; schema должна быть новой:

```json
{"action": "build", "mode": "initial", "schema": "oil_r_demo_initial"}
```

INITIAL удерживает живой source fence, проверяет STG→DDS/checkpoints, фиксирует baseline и raw snapshots, строит 21 таблицу, сверяет и регистрирует READY. Для обновления сначала доставляют новые Kafka records в raw и применяют их в GP, затем:

```json
{"action": "build", "mode": "refresh", "schema": "oil_r_demo_refresh", "initial_schema": "oil_r_demo_initial"}
```

REFRESH использует **тот же INITIAL baseline**, новый raw snapshot и полные offset-окна. Missing offset или расхождение блокируют выпуск. Manifest, baseline и snapshots сохраняются на весь срок обновлений.

В LakeKeeper UI выдать CIAM-роли `oil_dlh_reader` table-level `select` на `agg_field_day` и `field_performance` нового release. Затем PUBLISH, если текущий dataset 8 действительно указывает на template:

```json
{"action": "publish", "schema": "oil_r_demo_initial", "dataset_id": 8, "database_id": 2, "expected_schema": "oil_demo_template", "expected_table": "field_performance", "table": "field_performance"}
```

Verifier независимо проверяет grants и snapshots; publisher меняет только разрешённый dataset. Для REFRESH публикации укажите текущий INITIAL schema в `expected_schema`. Каталожные шаги повторяются отдельно: `catalog_ready` / `catalog_published`. Source/target Kafka переключаются оператором; DAG не запускает генератор.

Superset: dataset 6 / dashboard 5 — frozen первая волна; dataset 8 / dashboard 6 — вторая. Первый отчёт не обновляется автоматически при REFRESH второй цепочки. Reader читает marts, engineer обрабатывает три слоя; BI RBAC и service identity соединения регулируются отдельно.

INITIAL, обратимый PUBLISH и REFRESH с пустым новым окном проверены. Полный непустой REFRESH основного topic оставлен для вебинара. Это отдельные Iceberg commits, не распределённая транзакция. Kafka RF=1; Spark/Jupyter не исполняют выбранный маршрут. Ops patches переживают pod restart, после Helm/XaaS upgrade требуется reapply. Секреты, inventories, manifests, evidence и отчёты запусков в Git не хранятся. Исходный контур: [dwh-greenplum-scripts](https://github.com/ncuho/dwh-greenplum-scripts).
