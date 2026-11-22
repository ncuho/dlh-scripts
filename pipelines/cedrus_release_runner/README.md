# cedrus_release_runner

Создаёт 21 release-таблицу в порядке зависимостей через Cedrus. Проверяет snapshots, непрерывность offsets и результаты; сохраняет evidence, поддерживает ограниченное восстановление.

Исходный код: [cedrus_release_runner.py](cedrus_release_runner.py).

## Статус сущности

- **GitHub:** код и описание находятся в этой папке.
- **Data Catalog:** Участие отражено целевым DAG и lineage создаваемых таблиц.
- **Lakehouse:** Исполняемый runtime установлен; создаёт таблицы только по явному запуску.
