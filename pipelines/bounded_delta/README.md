# bounded_delta

Проверяет структуру manifest, идентификаторы таблиц, snapshots и offset-диапазоны; атомарно сохраняет evidence.

Исходный код: [bounded_delta.py](bounded_delta.py).

## Статус сущности

- **GitHub:** код и описание находятся в этой папке.
- **Data Catalog:** Участие отражено целевым DAG и lineage создаваемых таблиц.
- **Lakehouse:** Исполняемый runtime установлен; создаёт таблицы только по явному запуску.
