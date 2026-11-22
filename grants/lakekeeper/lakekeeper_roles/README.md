# lakekeeper_roles

Сопоставляет CIAM-backed reader/engineer роли с разрешениями LakeKeeper через штатный Management API.

Исходный код: [lakekeeper_roles.py](lakekeeper_roles.py).

## Статус сущности

- **GitHub:** код и описание находятся в этой папке.
- **Data Catalog:** Управляются нативными GP/Kafka/CIAM/LakeKeeper механизмами; отдельной карточки grant в Data Catalog нет.
- **Lakehouse:** GP GRANT не копируются буквально. Целевые CIAM-backed роли и LakeKeeper permissions настроены отдельно.
