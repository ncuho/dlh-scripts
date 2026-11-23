-- Назначение: Spark SQL transformation over the immutable release input. Session timezone is UTC; INITIAL and REFRESH must use a verified cut and snapshots.
-- Исполняйте в указанном SQL-движке; одноимённые функции и типы GP и Spark могут иметь разную семантику.
SELECT
  reason,
  description
FROM VALUES
  ('EQUIPMENT', 'Оборудование'),
  ('POWER', 'Энергоснабжение'),
  ('PLANNED', 'Плановый простой') AS v(reason, description)
