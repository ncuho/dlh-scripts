# Исторический вход без остановки GP

Ручной DAG `oil_source_snapshot` с одной task `export_source`. Конфигурация — `{"mode":"initial"}` для C0 или `{"mode":"refresh"}` для C1. Task ожидает согласованный бизнес-срез, открывая новые read-only snapshots без остановки источника; если backlog не успел примениться, источник продолжает работу, а задача ограниченно ждёт.

Один native GP connection читает все двадцать объектов в REPEATABLE READ READ ONLY. Cedrus записывает извлечённые значения в техническую таблицу Bronze, затем SHA-256 каждого блока сверяется с источником и закрепляется Iceberg snapshot. На выходе — SEALED и JSON `next_dag_config` для `oil_layered_migration`. Ошибка или неизвестный результат не допускаются к бизнес-загрузке; автоматических retries нет.
