-- Контекст миграции: AS IS / TO BE.
-- Назначение: Хранит устойчивый бизнес-ключ и hash key сущности field; атрибуты вынесены в satellites.
-- AS IS: oil_dds.hub_field в GP; выбранная исходная бизнес-сущность.
-- TO BE: REFRESH: отдельный выпуск из pinned INITIAL и полного raw-окна [C0,C1).
-- Отличие: INITIAL не изменяется; source GP для пересчёта не перечитывается. Версии/удаления применяет целевой SQL.
-- Конец контекста миграции.
-- Хранит бизнес-идентификатор месторождения и детерминированный hash key. Не содержит изменяемых описательных атрибутов.
-- Engine: spark. Столбцы INSERT берутся в порядке DDL, не по позиции исходного SELECT.
-- REFRESH: закреплённый INITIAL + весь диапазон C0–C1; исключены более поздние offsets.
WITH candidates AS
    (SELECT md5(concat('field:', e.entity_id)) AS field_hk,
            e.entity_id AS field_id,{{cut_time}} AS load_dts,
                                               min(e.record_source) AS record_source
     FROM {{target:oil_dds__event}} e
     WHERE e.event_type='field'
         AND NOT EXISTS
             (SELECT 1
              FROM {{initial:oil_dds__event}} i
              WHERE i.event_id=e.event_id)
     GROUP BY e.entity_id)
SELECT field_hk,
       field_id,
       load_dts,
       record_source
FROM {{initial:oil_dds__hub_field}}
UNION ALL
SELECT c.field_hk,
       c.field_id,
       c.load_dts,
       c.record_source
FROM candidates c
WHERE NOT EXISTS
        (SELECT 1
         FROM {{initial:oil_dds__hub_field}} i
         WHERE i.field_id=c.field_id);
