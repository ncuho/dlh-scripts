# package_runtime

Собирает categorized source в плоский каталог managed Airflow, сохраняя действующие имена импортов. Публикацию и перезапуск сервисов не выполняет.

`python deploy/package_runtime/package_runtime.py --output /tmp/oil-runtime-package`

## Статус сущности

- **GitHub:** код и описание находятся в этой папке.
- **Data Catalog:** Отдельной карточки нет; служебная реализация описана в коде и документации.
- **Lakehouse:** Не переносится как объект данных; используется для эксплуатации или настройки.
