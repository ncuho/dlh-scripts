-- Контекст миграции: AS IS / TO BE.
-- Назначение: Формирует SCD2-версии месторождений с интервалом действия.
-- AS IS: PL/pgSQL GP строит Vault-проекции и Kimball-модель внутри БД.
-- TO BE: Spark SELECT строит сущность из canonical history в согласованном release.
-- Отличие: Этот набор использует альтернативную canonical-модель; текущий production_online нельзя подменять им без review.
-- Конец контекста миграции.
SELECT
  LOWER(HEX(UNHEX(MD5(ENCODE('field:' || entity_id, 'utf-8'))))) AS field_hk,
  entity_version,
  event_time AS valid_from,
  LEAD(event_time) OVER (
    PARTITION BY entity_id
    ORDER BY event_time NULLS LAST, entity_version NULLS LAST, event_id NULLS LAST
  ) AS valid_to,
  GET_JSON_OBJECT(document, '$.payload.field_name') AS field_name,
  GET_JSON_OBJECT(document, '$.payload.region') AS region,
  (
    op = 'D'
  ) AS is_deleted
FROM dlh.${schema}.canonical_event
WHERE
  event_type = 'field'
