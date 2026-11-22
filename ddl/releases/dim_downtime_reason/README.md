# dim_downtime_reason

Справочник причин: EQUIPMENT, POWER, PLANNED. Нормализует группировку простоев.

Контур: immutable release второй цепочки; параметры берутся из согласованного manifest.

Исходный код: [dim_downtime_reason.sql](dim_downtime_reason.sql).

## Статус сущности

- **GitHub:** код и описание находятся в этой папке.
- **Data Catalog:** Будет зарегистрирована автоматически после успешного INITIAL/REFRESH и проверок READY.
- **Lakehouse:** SQL подготовлен; изолированные выпуски проверены и удалены, постоянного экземпляра сейчас нет.
