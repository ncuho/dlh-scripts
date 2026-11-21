# Реестр артефактов

| Группа | Файлы | Роль |
|---|---|---|
| Airflow | `runtime/airflow_oil_wave2.py` | Ручное управление действиями build, catalog_ready, publish и catalog_published; build поддерживает initial/refresh. |
| Граница | `runtime/source_fence.py`, `fence_controller.py`, `fence_probe_socket.py` | Подготовка и удержание границы, проверка её состояния. |
| Данные и качество | `runtime/bounded_delta.py`, `cedrus_release_runner.py`, `sql_plan.py`, `dq_sql.py` | Вход, преобразования, проверка и сверка. |
| Публикация | `runtime/publish_dataset.py`, `superset_auth.py`, `runtime_catalog.py` | Привязка Superset dataset и описание выпуска в каталоге. |
| Служебная утилита | `runtime/grant_serving.py` | Административная выдача прав; не размещена в исполняемом runtime Airflow, не используется проверяющим сервисом. |
| Проверки | `quality/` | Имеющиеся материалы первой волны. |
| Администрирование | `ops/configure_runtime_tls.py`, `configure_fence_relay.py`, `lakekeeper_roles.py`, `lakekeeper_verifier.py`, `configure_lakekeeper_verifier.py`, `catalog_launcher_overlay.py`, `deploy_catalog_overlay.py` | Подготовка маршрута, fence и ролей администраторами. |
| Контракт событий | `metadata/oil-event-json-schema.json` | JSON-схема типов события; соответствие фактическому topic требует отдельной проверки на дату выпуска. |
| Источник и Kafka | Greenplum-репозиторий: `gp/`, `ingestion/`, `ingestion/wave2/` | DDL, бизнес-функции, producer, consumer и отдельный wave2 worker. |

Список описывает назначение, не подтверждает успешное развёртывание каждого файла. Копия runtime должна быть перед выпуском сверена по SHA-256 с фактическими загруженными версиями: исходные файлы продолжают дорабатываться. Исходные data/snapshot backup, сведения о lease, ключи, секреты и внутренние журналы в этот реестр не включаются.
