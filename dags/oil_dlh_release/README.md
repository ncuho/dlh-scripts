# oil_dlh_release

Один DAG для первой сверки и постоянной обработки цепочки добычи. Одна task `apply_events_and_refresh_marts` вызывает `release()` → `oil_dlh_jobs.execute(mode, ...)`.

1. После принятого INITIAL запустите вручную с `{"mode":"catchup","overlap_offsets":10}`: согласованный GP C1, raw overlap, Spark Bronze → Vault → Kimball, 58 проверок со сверкой Gold с GP. Даже при C1=C0 выполняется приёмка.
2. После Success/READY оставьте DAG включённым: cron `*/1 * * * *`, конфигурация `{}` означает mode=release, raw → слои через Spark, 53 проверки без чтения GP. Если C1 ещё не принят, обычный run получает Skipped без записи.

`max_active_runs=1`, pool `oil_dlh_writer`, файловая блокировка и snapshots сохраняются. Одновременно выполняется один writer. Во время первого ручного запуска можно отметить Unpause on trigger: ранние scheduled runs пропускаются до приёмки C1. Не задавайте catchup через постоянную конфигурацию расписания.

`prepare`, `materialize`, `validate`, `accept`, `rollback` находятся в `pipelines/oil_layered_migration/oil_dlh_jobs.py`; SQL каждой сущности — в `pipelines/oil_dlh_release`. Serving записывается последним, общей транзакции 20 таблиц Iceberg нет. Ветка простоев остаётся в GP.
