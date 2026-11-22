# configure_fence_relay

Настраивает ограниченный SSH relay от Airflow к Kafka fence с forced command и закреплёнными host keys.

Исходный код: [configure_fence_relay.py](configure_fence_relay.py).

До запуска задайте `OIL_GP_SSH_CONFIG` — путь к существующей SSH-конфигурации административного доступа.

## Статус сущности

- **GitHub:** код и описание находятся в этой папке.
- **Data Catalog:** Отдельной карточки вспомогательного runtime нет; назначение отражено DAG/описанием интеграции.
- **Lakehouse:** Механизм установлен; не является переносимой бизнес-таблицей.
