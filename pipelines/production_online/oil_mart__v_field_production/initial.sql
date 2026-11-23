-- Контекст миграции: AS IS / TO BE.
-- Назначение: Предоставляет бизнес-колонки отчёта добычи по месторождениям.
-- AS IS: oil_mart.v_field_production в GP; выбранная исходная бизнес-сущность.
-- TO BE: INITIAL C0: перенос сохранённой исходной истории или расчёт производной проекции по dependencies.
-- Отличие: Источник раскрывается в immutable capture; историю аудита копирует Cedrus, производные Marts считает Spark.
-- Конец контекста миграции.
-- Serving-представление первой цепочки поверх agg_field_day для отчёта добычи.
-- Engine: spark. Столбцы INSERT берутся в порядке DDL, не по позиции исходного SELECT.
-- INITIAL: согласованный исходный образ C0; исторический аудит не пересоздаётся.
SELECT production_date,
       field_hk,
       field_name,
       region,
       oil_tonnes,
       producing_well_days,
       oil_per_well_day
FROM {{target:oil_mart__agg_field_day}};
