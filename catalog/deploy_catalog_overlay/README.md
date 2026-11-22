# deploy_catalog_overlay

Подключает versioned overlay к MCP через ConfigMap. После Helm/XaaS reconcile требуется сверка и при необходимости reapply.

Исходный код: [deploy_catalog_overlay.py](deploy_catalog_overlay.py).

## Статус сущности

- **GitHub:** код и описание находятся в этой папке.
- **Data Catalog:** Это код обслуживания каталога; отдельная бизнес-карточка Python-модуля не создаётся.
- **Lakehouse:** Runtime registration и MCP overlay установлены; обслуживают целевой выпуск и BI lineage.
