-- Контекст миграции: AS IS / TO BE.
-- Назначение: Формирует SCD2-версии скважин и принадлежность месторождению.
-- AS IS: oil_mart.dim_well в GP; выбранная исходная бизнес-сущность.
-- TO BE: REFRESH: отдельный выпуск из pinned INITIAL и полного raw-окна [C0,C1).
-- Отличие: INITIAL не изменяется; source GP для пересчёта не перечитывается. Версии/удаления применяет целевой SQL.
-- Конец контекста миграции.
-- Историческое измерение скважины, её принадлежности месторождению и статуса. Версия выбирается на производственную дату.
-- Engine: spark. Столбцы INSERT берутся в порядке DDL, не по позиции исходного SELECT.
-- REFRESH: закреплённый INITIAL + весь диапазон C0–C1; исключены более поздние offsets.
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
