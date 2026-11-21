# Миграция Greenplum → Lakehouse

Первая цепочка добычи перенесена CedrusData Engine в 14 Iceberg tables и опубликована в Superset. Повторная двусторонняя сверка всех 14 объектов дала ноль различий. Вторая цепочка эффективности и простоев подготовлена к показу: её connector PAUSED, raw и панель пустые до действий ведущего.

`runtime/` — фактический Cedrus/Airflow release workflow: immutable INITIAL baseline, ограниченная дельта REFRESH, DQ/EXCEPT ALL, controlled PUBLISH и Data Catalog lineage. `ops/` — versioned patches и доступы; после Helm/XaaS upgrade они применяются повторно. `dlh/` — альтернативный Spark-проект, не выбранный проверенный runtime. `sql/` и `quality/` содержат первую волну и сверки. Секреты вне Git.

Начните с [22 шагов вебинара](docs/WEBINAR-22-STEPS.md), [состояния и ограничений](docs/02-фактический-статус.md), [сервисов](docs/SERVICE-INDEX.md), [доступов CIAM/LakeKeeper](docs/ACCESS-UI-RUNBOOK.md) и [runtime README](runtime/README.md). Отдельные Iceberg commits не образуют одной транзакции, Kafka RF=1; промышленная HA не заявляется.
