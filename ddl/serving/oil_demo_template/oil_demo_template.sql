-- Контекст миграции: AS IS / TO BE.
-- Назначение: Namespace пустой serving-таблицы, используемой до первого подтверждённого выпуска второй цепочки.
-- AS IS: GP предоставляет view конечному отчёту.
-- TO BE: Cedrus предоставляет target serving над принятым Iceberg release.
-- Отличие: Binding меняется после сверок и grants; исходный отчёт не удаляется.
-- Конец контекста миграции.
CREATE SCHEMA IF NOT EXISTS dlh.oil_demo_template;
