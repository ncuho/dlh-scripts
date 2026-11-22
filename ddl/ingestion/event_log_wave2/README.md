# event_log_wave2

Отдельная raw Iceberg-таблица Kafka sink. Хранит Kafka-координаты и полный JSON, не изменяя ранее принятый baseline первой цепочки.

Исходный код: [event_log_wave2.sql](event_log_wave2.sql).

## Статус сущности

- **GitHub:** код и описание находятся в этой папке.
- **Data Catalog:** Зарегистрирована отдельная карточка raw-таблицы.
- **Lakehouse:** Создана; пустая, connector PAUSED.
