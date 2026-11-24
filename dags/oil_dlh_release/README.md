# oil_dlh_release

Аналог ветки нефтедобычи oil_gp_release: каждую минуту применяет raw → Stage → Data Vault → Kimball в Spark. Greenplum не читает. Ветка простоев остаётся в исходном GP.

JSON `{}` или `{"overlap_offsets":10}`. Общий pool `oil_dlh_writer`: один writer. Порядок PK/FK, DQ и rollback snapshots реализован в `pipelines/oil_layered_migration/oil_dlh_jobs.py`. SQL каждой сущности находится в `pipelines/oil_dlh_release`. Serving публикуется последним; общей транзакции 20 таблиц Iceberg нет.
