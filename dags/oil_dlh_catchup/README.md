# oil_dlh_catchup

Ручной догон существующих таблиц на C1 из raw с overlap и сравнением Gold с GP в одном read-only snapshot.

JSON `{}` или `{"overlap_offsets":10}`. Общий pool `oil_dlh_writer`: один writer. Порядок PK/FK, DQ и rollback snapshots реализован в `pipelines/oil_layered_migration/oil_dlh_jobs.py`. SQL каждой сущности находится в `pipelines/oil_dlh_release`. Serving публикуется последним; общей транзакции 20 таблиц Iceberg нет.
