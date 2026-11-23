-- Контекст миграции: AS IS / TO BE.
-- Назначение: Хранит историю связи production_day_well через hash keys связанных hubs.
-- AS IS: PL/pgSQL GP строит Vault-проекции и Kimball-модель внутри БД.
-- TO BE: Spark SELECT строит сущность из canonical history в согласованном release.
-- Отличие: Этот набор использует альтернативную canonical-модель; текущий production_online нельзя подменять им без review.
-- Конец контекста миграции.
SELECT DISTINCT
  LOWER(
    HEX(
      UNHEX(
        MD5(
          ENCODE(
            'production_day_well:' || entity_id || ':' || GET_JSON_OBJECT(document, '$.payload.well_id'),
            'utf-8'
          )
        )
      )
    )
  ) AS link_hk,
  LOWER(HEX(UNHEX(MD5(ENCODE('production:' || entity_id, 'utf-8'))))) AS production_hk,
  LOWER(
    HEX(
      UNHEX(MD5(ENCODE('well:' || GET_JSON_OBJECT(document, '$.payload.well_id'), 'utf-8')))
    )
  ) AS well_hk
FROM dlh.${schema}.canonical_event
WHERE
  event_type = 'production_day'
