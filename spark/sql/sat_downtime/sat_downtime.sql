-- Контекст миграции: AS IS / TO BE.
-- Назначение: Хранит версии атрибутов downtime, происхождение, порядок версий и операции изменения.
-- AS IS: PL/pgSQL GP строит Vault-проекции и Kimball-модель внутри БД.
-- TO BE: Spark SELECT строит сущность из canonical history в согласованном release.
-- Отличие: Этот набор использует альтернативную canonical-модель; текущий production_online нельзя подменять им без review.
-- Конец контекста миграции.
SELECT
  LOWER(HEX(UNHEX(MD5(ENCODE('downtime:' || entity_id, 'utf-8'))))) AS downtime_hk,
  event_id,
  entity_version,
  op,
  event_time,
  document
FROM dlh.${schema}.canonical_event
WHERE
  event_type = 'downtime'
