# Lakehouse: миграция Greenplum в Iceberg

Целевой код демо миграции аналитической цепочки из Greenplum в Lakehouse на VK Data Platform. Iceberg хранит данные в S3, LakeKeeper предоставляет REST-каталог, CedrusData Engine использует SQL-диалект Trino, Spark выполняет бизнес-преобразования, Airflow управляет загрузкой и проверками.

Кейс — отчёт по добыче нефти из [исходного DWH на Greenplum](https://github.com/ncuho/dwh-greenplum-scripts). Переносятся 20 объектов в слои **Bronze, Silver и Gold** с сохранением бизнес-ключей, истории и логики расчётов. Входная таблица Kafka raw создаётся отдельно.

Сценарий включает копирование истории из согласованного среза GP, догон накопившихся событий, регулярную обработку и отдельный отчёт Superset. Исходный GP продолжает работать. Полноту переноса, ключи, связи и бизнес-показатели проверяем до принятия результата; общей транзакции на все Iceberg-таблицы нет.

## Что внутри

| Каталог | Содержание |
|---|---|
| [architecture/](architecture/) | Архитектура целевого контура и модели данных. |
| [ddl/](ddl/) и [pipelines/production_online/](pipelines/production_online/) | DDL Iceberg, SQL-шаблоны загрузки и проверки по объектам. |
| [dags/oil_layered_migration/](dags/oil_layered_migration/) | Первичная загрузка истории: Python читает GP, Cedrus записывает Iceberg. |
| [dags/oil_dlh_release/](dags/oil_dlh_release/) | Догон и регулярная бизнес-обработка через Spark. |
| [pipelines/oil_layered_migration/](pipelines/oil_layered_migration/) | Исполнение загрузки, работа со snapshots, сверки и восстановление после ошибок. |
| [pipelines/oil_dlh_release/](pipelines/oil_dlh_release/) | Spark SQL для слоёв, измерений, фактов и агрегатов. |
| [queries/](queries/) | Запросы для анализа и проверки результатов. |
| [grants/](grants/), [catalog/](catalog/) и [superset/](superset/) | Права LakeKeeper, метаданные и lineage, определения BI-объектов. |
| [tools/](tools/) | Конвертер SQL GP в Trino, ручной запуск обработки и сброс результатов демо. |
| [agents/gp_lakehouse/](agents/gp_lakehouse/) | Инструкции, инструменты и проверенные шаблоны агента миграции для AI Space. |

Начните с двух DAG и их реализаций в `pipelines/`: так проще проследить путь от исходных строк до отчёта. Внутри папок сущностей есть пояснения и комментарии к коду.

Для запуска нужны настроенные сервисы, подключения и права доступа. В репозитории также сохранены материалы второй цепочки и прежних вариантов миграции; основной маршрут добычи указан выше.
