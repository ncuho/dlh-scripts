# AS IS → TO BE

20 предметных объектов: 6 Stage → bronze, 9 Vault → silver, 5 Marts → gold. Ещё одна raw-таблица принимает Kafka отдельно и не является 21-м source-объектом миграции.

DDL: типы/колонки соответствуют утверждённым 20 целям. GP DISTRIBUTED BY/индексы не переносятся в Iceberg; partitioning не выдумывается. FORMAT PARQUET, format_version=2. VARCHAR вместо text, timestamp(6) with time zone вместо timestamptz, decimal(18,3) сохраняет точность. View materialized как таблицы по явному колонночному контракту. NOT NULL/PK/FK/defaults не считаются принудительно обеспеченными Iceberg; их правила переносятся в ingestion/DQ и порядок зависимостей.

AS IS: oil_gp_release, одна task apply_batches_and_refresh_marts, cron */1 * * * *. apply_ready_batches вызывает oil_dds.apply_batch и oil_mart.refresh_production в транзакции GP; refresh_downtime обслуживает вторую ветку.

TO BE: oil_production_spark_candidate, одна task apply_events_and_refresh_marts, тот же cron, max_active_runs=1, retries=0, pool oil_dlh_writer. Python вызывает execute('release'), Spark выполняет 20 операций и 53 DQ. Bronze/Silver используют MERGE с ключами, Gold — пересчёт/overwrite, serving записывается последней. Родитель VALIDATED до ребёнка. Подтверждённые ошибки компенсируются snapshots; неизвестный исход блокирует writers. Общей ACID-транзакции двадцати Iceberg-таблиц нет.

Профиль переносит ветку добычи, не весь oil_gp_release: refresh_downtime остаётся в GP. История copied отдельным INITIAL через Cedrus; release не читает GP, а обрабатывает уже доставленный DLH raw с overlap и checkpoint. При запускаемом будущем run должны существовать целевые таблицы, принятый INITIAL, raw, connection/secret files и shared runtime /opt/airflow/dags/managed/oil_layered_migration. Пакет содержит этот runtime и бизнес-SQL для проверки/установки; регистрация одного DAG не создаёт таблицы/права/данные.

Candidate — отдельный ID для инспекции, чтобы не заменить действующий source/target DAG. Создаётся на паузе. В этой задаче запрещены trigger/unpause/SQL execution. SHA-256 фиксирует source и весь reviewed target profile. Это воспроизводимый проверенный перевод известного кейса; новые процедуры требуют нового профиля и проверки, а не ложной универсальной трансляции PL/pgSQL.
