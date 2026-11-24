# Исторический вход без остановки GP

Ручной DAG `oil_source_snapshot`, task `export_source`. Конфигурация `{"mode":"initial"}` для C0 или `{"mode":"refresh"}` для C1. Один native GP connection читает все двадцать объектов в REPEATABLE READ READ ONLY.

Граница — конец непрерывной уже применённой истории, а не received checkpoint и не ожидание пустой очереди. READY-партии остаются за границей для последующего raw-догона. Шесть Stage-объектов проецируются на APPLIED-партии с их coalesce-аудитом и applied checkpoint; DDS/Marts читаются целиком из того же MVCC snapshot. Manifest сохраняет обе границы, pending count и фактический source_projection каждого объекта. Пропуски, повторы, применение событий вне префикса и расхождение Stage/ledger блокируют экспорт.

Cedrus записывает извлечённые Python значения в техническую таблицу Bronze. SHA-256 блоков, количество и координаты сверяются; закреплённая Iceberg version получает SEALED. На выходе `next_dag_config` для `oil_layered_migration`. GP продолжает работу; записи, паузы и миграционные схемы в GP не создаются. Новый DAG run получает новый export_id; Clear неуспешного экспорта без сверки не используйте.
