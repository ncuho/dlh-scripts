-- Контекст миграции: AS IS / TO BE.
-- Назначение: Сохраняет происхождение доставки после объединения партий.
-- AS IS: oil_stg.batch_coalesce_event_map в GP; выбранная исходная бизнес-сущность.
-- TO BE: REFRESH: отдельный выпуск из pinned INITIAL и полного raw-окна [C0,C1).
-- Отличие: Сохраняется аудит GP на C0; это не новый checkpoint Iceberg consumer и не live source C1.
-- Конец контекста миграции.
-- Сохраняет соответствие каждой Kafka-позиции исходной и объединённой партиям.
-- Engine: trino. Столбцы INSERT берутся в порядке DDL, не по позиции исходного SELECT.
-- REFRESH: закреплённый INITIAL + весь диапазон C0–C1; исключены более поздние offsets.
SELECT topic,
       kafka_partition,
       kafka_offset,
       original_batch_id,
       coalesced_batch_id
FROM {{initial:oil_stg__batch_coalesce_event_map}};
