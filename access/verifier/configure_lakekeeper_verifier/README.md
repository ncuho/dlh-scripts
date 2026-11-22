# configure_lakekeeper_verifier

Разворачивает verifier с TLS, авторизацией и ограничением сетевого доступа. Секреты хранятся в Kubernetes Secret.

Исходный код: [configure_lakekeeper_verifier.py](configure_lakekeeper_verifier.py).

## Статус сущности

- **GitHub:** код и описание находятся в этой папке.
- **Data Catalog:** Отдельной карточки вспомогательного runtime нет; назначение отражено DAG/описанием интеграции.
- **Lakehouse:** Механизм установлен; не является переносимой бизнес-таблицей.
