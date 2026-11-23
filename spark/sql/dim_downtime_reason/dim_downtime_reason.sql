-- Контекст миграции: AS IS / TO BE.
-- Назначение: Определяет справочник причин простоя для размерной модели.
-- AS IS: PL/pgSQL GP строит Vault-проекции и Kimball-модель внутри БД.
-- TO BE: Spark SELECT строит сущность из canonical history в согласованном release.
-- Отличие: Этот набор использует альтернативную canonical-модель; текущий production_online нельзя подменять им без review.
-- Конец контекста миграции.
SELECT
  reason,
  description
FROM VALUES
  ('EQUIPMENT', 'Оборудование'),
  ('POWER', 'Энергоснабжение'),
  ('PLANNED', 'Плановый простой') AS v(reason, description)
