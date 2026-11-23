-- Контекст миграции: AS IS / TO BE.
-- Назначение: Предоставляет бизнес-колонки отчёта добычи по месторождениям.
-- AS IS: oil_mart.v_field_production в GP; выбранная исходная бизнес-сущность.
-- TO BE: REFRESH: отдельный выпуск из pinned INITIAL и полного raw-окна [C0,C1).
-- Отличие: INITIAL не изменяется; source GP для пересчёта не перечитывается. Версии/удаления применяет целевой SQL.
-- Конец контекста миграции.
-- Serving-представление первой цепочки поверх agg_field_day для отчёта добычи.
-- Engine: spark. Столбцы INSERT берутся в порядке DDL, не по позиции исходного SELECT.
-- REFRESH: закреплённый INITIAL + весь диапазон C0–C1; исключены более поздние offsets.
SELECT production_date,
       field_hk,
       field_name,
       region,
       oil_tonnes,
       producing_well_days,
       oil_per_well_day
FROM {{target:oil_mart__agg_field_day}};
