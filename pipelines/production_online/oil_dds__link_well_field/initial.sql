-- Контекст миграции: AS IS / TO BE.
-- Назначение: Хранит историю связи well_field через hash keys связанных hubs.
-- AS IS: oil_dds.link_well_field в GP; выбранная исходная бизнес-сущность.
-- TO BE: INITIAL C0: перенос сохранённой исходной истории или расчёт производной проекции по dependencies.
-- Отличие: Источник раскрывается в immutable capture; историю аудита копирует Cedrus, производные Marts считает Spark.
-- Конец контекста миграции.
-- Хранит историческую связь скважины с месторождением по техническим ключам. Изменяемые атрибуты находятся в satellites.
-- Engine: trino. Столбцы INSERT берутся в порядке DDL, не по позиции исходного SELECT.
-- INITIAL: согласованный исходный образ C0; исторический аудит не пересоздаётся.
SELECT link_hk,
       well_hk,
       field_hk,
       load_dts,
       record_source
FROM {{source:oil_dds__link_well_field}};
