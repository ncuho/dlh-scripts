SELECT
  reason,
  description
FROM VALUES
  ('EQUIPMENT', 'Оборудование'),
  ('POWER', 'Энергоснабжение'),
  ('PLANNED', 'Плановый простой') AS v(reason, description)
