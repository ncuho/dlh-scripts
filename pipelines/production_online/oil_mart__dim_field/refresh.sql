-- Контекст миграции: AS IS / TO BE.
-- Назначение: Формирует SCD2-версии месторождений с интервалом действия.
-- AS IS: oil_mart.dim_field в GP; выбранная исходная бизнес-сущность.
-- TO BE: REFRESH: отдельный выпуск из pinned INITIAL и полного raw-окна [C0,C1).
-- Отличие: INITIAL не изменяется; source GP для пересчёта не перечитывается. Версии/удаления применяет целевой SQL.
-- Конец контекста миграции.
-- Историческое измерение месторождения. Интервал действия версии — [valid_from, valid_to); удаление сохраняется отдельным признаком.
-- Engine: spark. Столбцы INSERT берутся в порядке DDL, не по позиции исходного SELECT.
-- REFRESH: закреплённый INITIAL + весь диапазон C0–C1; исключены более поздние offsets.
SELECT field_hk,
       entity_version,
       event_time AS valid_from,
       lead(event_time) OVER (PARTITION BY field_hk
                              ORDER BY event_time, entity_version) AS valid_to,
       field_name,
       region,
       op='D' AS is_deleted
FROM {{target:oil_dds__sat_field}};
