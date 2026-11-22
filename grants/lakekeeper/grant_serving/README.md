# grant_serving

Admin-утилита назначения reader SELECT на две serving-таблицы конкретного готового release; проверяет snapshots. Основной операторский способ — LakeKeeper UI.

Исходный код: [grant_serving.py](grant_serving.py).

## Статус сущности

- **GitHub:** код и описание находятся в этой папке.
- **Data Catalog:** Управляются нативными GP/Kafka/CIAM/LakeKeeper механизмами; отдельной карточки grant в Data Catalog нет.
- **Lakehouse:** GP GRANT не копируются буквально. Целевые CIAM-backed роли и LakeKeeper permissions настроены отдельно.
