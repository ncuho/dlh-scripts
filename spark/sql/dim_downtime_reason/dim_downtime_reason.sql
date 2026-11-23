-- Формируем согласованные коды причин простоев отдельной размерной таблицей. Это целевой SQL-этап вместо части apply_batch GP.
SELECT
  reason,
  description
FROM VALUES
  ('EQUIPMENT', 'Оборудование'),
  ('POWER', 'Энергоснабжение'),
  ('PLANNED', 'Плановый простой') AS v(reason, description)
