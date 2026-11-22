# grant_ingestion

Назначает отдельной CIAM service identity право modify только на выделенную raw-таблицу Iceberg.

Исходный код: [grant_ingestion.py](grant_ingestion.py).

## Статус сущности

- **GitHub:** код и описание находятся в этой папке.
- **Data Catalog:** Управляются нативными GP/Kafka/CIAM/LakeKeeper механизмами; отдельной карточки grant в Data Catalog нет.
- **Lakehouse:** GP GRANT не копируются буквально. Целевые CIAM-backed роли и LakeKeeper permissions настроены отдельно.
