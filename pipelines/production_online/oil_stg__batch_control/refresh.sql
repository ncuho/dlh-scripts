-- Контекст миграции: AS IS / TO BE.
-- Назначение: Учитывает состояния исходных партий и завершённость применения.
-- AS IS: oil_stg.batch_control в GP; выбранная исходная бизнес-сущность.
-- TO BE: REFRESH: отдельный выпуск из pinned INITIAL и полного raw-окна [C0,C1).
-- Отличие: Сохраняется аудит GP на C0; это не новый checkpoint Iceberg consumer и не live source C1.
-- Конец контекста миграции.
-- Хранит жизненный цикл партии: OPEN, READY, COALESCED, APPLIED или FAILED. Отделяет успешную доставку от применения в DDS.
-- Engine: trino. Столбцы INSERT берутся в порядке DDL, не по позиции исходного SELECT.
-- REFRESH: закреплённый INITIAL + весь диапазон C0–C1; исключены более поздние offsets.
SELECT batch_id,
       created_at,
       error_text,
       source_manifest,
       source_name,
       status
FROM {{initial:oil_stg__batch_control}};
