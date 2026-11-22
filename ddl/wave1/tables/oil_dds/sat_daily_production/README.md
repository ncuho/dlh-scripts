# sat_daily_production

Сохраняет версии атрибутов дневной добычи, event_id, entity_version и операцию U/D. GP хранит типизированный payload и audit-поля; release DLH использует логическую проекцию с исходным document.

Контур: уже перенесённая первая цепочка, зафиксированный baseline.

Исходный код: [sat_daily_production.sql](sat_daily_production.sql).

## Статус сущности

- **GitHub:** код и описание находятся в этой папке.
- **Data Catalog:** Схемы и 14 таблиц первой цепочки зарегистрированы.
- **Lakehouse:** Применено; зафиксированный baseline первой цепочки.
