-- Контекст миграции: AS IS / TO BE.
-- Назначение: Формирует SCD2-версии скважин и принадлежность месторождению.
-- AS IS: oil_mart.dim_well в GP; выбранная исходная бизнес-сущность.
-- TO BE: INITIAL C0: перенос сохранённой исходной истории или расчёт производной проекции по dependencies.
-- Отличие: Источник раскрывается в immutable capture; историю аудита копирует Cedrus, производные Marts считает Spark.
-- Конец контекста миграции.
-- Историческое измерение скважины, её принадлежности месторождению и статуса. Версия выбирается на производственную дату.
-- Engine: spark. Столбцы INSERT берутся в порядке DDL, не по позиции исходного SELECT.
-- INITIAL: согласованный исходный образ C0; исторический аудит не пересоздаётся.
SELECT w.well_hk,
       l.field_hk,
       w.entity_version,
       w.valid_from,
       lead(w.valid_from) OVER (PARTITION BY w.well_hk
                                ORDER BY w.valid_from, w.entity_version) AS valid_to,
       w.well_name,
       w.status,
       w.op='D' AS is_deleted
FROM {{target:oil_dds__sat_well}} w
JOIN {{target:oil_dds__link_well_field}} l ON l.link_hk=w.link_hk;
