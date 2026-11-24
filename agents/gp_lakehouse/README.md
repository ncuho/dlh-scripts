# Мигратор GP в Lakehouse [вебинар]

Агент AI Space: https://vkaispace.io/app/1a0b925c-233f-4a4f-ba65-3987f497a85c/agents/3767da4b-31ed-40cf-ae59-f3a3aefdb6dc/settings

Модель по умолчанию `qwen38-27b-fp8`. Один агент, 12 итераций максимум, без сабагентов. Навыки `gp-lakehouse-v1` и штатный `dp-airflow`; существующее подключение `dp103-airflow`. Системная инструкция задаёт подготовку файлов, проверку и отдельную регистрацию без запуска.

## Как пользоваться

В AI Space → Агенты выберите «Мигратор GP в Lakehouse [вебинар]» → Начать чат.

1. «Переведи все 20 DDL текущей цепочки в Iceberg для Trino. Верни полный SQL-файл. Режим ddl. Ничего не выполняй».
2. «Разбери oil_gp_release, apply_batch и refresh_production. Подготовь соответствующую логику Airflow + Spark, полный пакет и diff. Режим all. Не публикуй».
3. «Зарегистрируй подготовленный oil_production_spark_candidate в Airflow. Оставь paused=true, проверь импорт. Не запускай DAG и не включай расписание».

Из результата скачайте `all_ddl.sql`, `migration-code.zip`, candidate DAG, `DIFF.md`, `result.json`. Полный DDL содержит создание трёх схем и 20 предметных таблиц. `raw_ingestion_ddl.sql` отдельно создаёт Kafka landing и не входит в 20 source-объектов.

## За счёт чего повторяется результат

Qwen выбирает режим и объясняет результат; Python формирует файлы из закреплённого проверенного профиля. В профиле — исходный GP-код и reviewed target DDL/Spark/runtime, точные SHA-256, порядок зависимостей и DQ. Сначала сверяются входные assets, затем строятся артефакты. ZIP использует фиксированные заголовки и ZIP_STORED, поэтому его байты не зависят от версии zlib на Mac/Linux. Произвольный текст модели не считается детерминированным: сравниваются именно файлы и хеши.

Это воспроизводимый перевод **известного текущего кейса**, не универсальный компилятор PL/pgSQL. Снимок исходников упакован в профиль; он не доказывает совпадение с изменившимся live GP. Для новой исходной версии обновите профиль через локальный `platform-bridge/scripts/build_gp_migration_skill.py` и повторите проверки. Другой CREATE TABLE агент переводит отдельным SQLGlot-скриптом `translate_ddl.py`; неизвестные типы, CTAS, произвольные views и процедуры требуют явного разбора.

## Что находится в пакете

- AS IS: source DDL, views, функции и исходный oil_gp_release.
- TO BE: 20 DDL, отдельный raw DDL, 20 apply.sql с DQ, Python-исполнитель/Spark transport и один candidate DAG.
- `DIFF.md`: типы, GP distribution/constraints, materialized views, SCD2, MERGE, порядок, сверки и компенсация.

Candidate повторяет ветку добычи действующего oil_dlh_release: cron `*/1 * * * *`, одна task `apply_events_and_refresh_marts`, `max_active_runs=1`, pool oil_dlh_writer. Часть refresh_downtime остаётся в GP: нельзя объявлять перенесёнными обе цепочки или весь исходный DAG без этого уточнения.

Для этого профиля MCP предоставляет `publish_reviewed_dag(artifact_id="oil-production-1.1")`. Сервер читает закреплённый DAG и проверяет SHA-256; модель не пересказывает Python-код в JSON-аргументе. Реализация и reviewed source находятся в `publisher/`. Обновление MCP установлено через отдельный ConfigMap и два read-only subPath mounts; исходные credentials/сервисы Airflow не меняются. При следующем Helm-релизе этот overlay нужно сохранить в штатных values, иначе reviewed tool исчезнет. Generic publish_dag для остальных задач сохраняет прежнее поведение.

Регистрация candidate не меняет действующие DAG и не исполняет job. Для будущего исполнения нужны принятый INITIAL, raw, целевые таблицы, grants и существующий shared runtime `/opt/airflow/dags/managed/oil_layered_migration`. В рамках подготовки агенту запрещены trigger/unpause и SQL execution. Инструкции не заменяют серверные ACL существующего MCP.

## Исходники

`skill/gp-lakehouse-v1/SKILL.md` — короткий workflow; `scripts/build.py` — renderer; `scripts/translate_ddl.py` — ограниченный DDL compiler; `assets` — версионированный source/target профиль. Это рабочие входы компиляции, а не метаданные конкретного исполнения. ZIP навыка дополнительно включает закреплённую библиотеку SQLGlot для автономного sandbox; third-party vendor code и run evidence не добавляются в репозиторий.

SQL ad-hoc конвертируется отдельным небольшим инструментом `tools/gp_sql_to_trino`. Пять исходных примеров лежат в `dwh-greenplum-scripts/queries/adhoc_gp`, целевые — в `dlh-scripts/queries/adhoc_trino`.

## Проверено 24.11.2026

Реальный Qwen-чат AI Space сформировал профиль `oil-production-1.1`: все 134 файла и ZIP побайтно совпали с локальной сборкой, SHA-256 ZIP `3fe8cc9ab1888fbb34e3840d9a2952e00508c031debe32c5dabcad9d64842133`. Проверка публикации через reviewed tool вернула `already_published`; Airflow подтвердил paused=true, has_import_errors=false, 0 запусков и совпадение source SHA с закреплённым DAG. Shared runtime существует. DLH содержит только системные схемы: DDL/DML и Spark при тесте не запускались.

Все пять GP ad-hoc выполнены read-only. Переводы Trino выполнены через Cedrus поверх того же GP connector и дали одинаковые строки; это проверка диалекта на исходных данных, не сверка уже мигрированного Iceberg. Исправлена важная разница JSON: text::json в GP соответствует JSON_PARSE(text) в Trino. Даты приёма событий явно приведены к UTC. Локальный набор проверок компиляторов, повторяемости, publisher, reset и release: 46 passed.

Первый generic publish через модель завершился ошибкой. Итоговый workflow заменён на серверный `publish_reviewed_dag` с artifact_id, проверен реальным вызовом агента. Публикация успешна и идемпотентна; автоматическое исполнение бизнес-цепочки в эту задачу не входит.
