-- Контекст миграции: AS IS / TO BE.
-- Назначение: Хранит устойчивый бизнес-ключ и hash key сущности well; атрибуты вынесены в satellites.
-- AS IS: PL/pgSQL GP строит Vault-проекции и Kimball-модель внутри БД.
-- TO BE: Spark SELECT строит сущность из canonical history в согласованном release.
-- Отличие: Этот набор использует альтернативную canonical-модель; текущий production_online нельзя подменять им без review.
-- Конец контекста миграции.
SELECT DISTINCT
  LOWER(HEX(UNHEX(MD5(ENCODE('well:' || entity_id, 'utf-8'))))) AS well_hk,
  entity_id
FROM dlh.${schema}.canonical_event
WHERE
  event_type = 'well'
