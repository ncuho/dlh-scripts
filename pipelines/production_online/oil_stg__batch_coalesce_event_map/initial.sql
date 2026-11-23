-- Контекст миграции: AS IS / TO BE.
-- Назначение: Сохраняет происхождение доставки после объединения партий.
-- AS IS: oil_stg.batch_coalesce_event_map в GP; выбранная исходная бизнес-сущность.
-- TO BE: INITIAL C0: перенос сохранённой исходной истории или расчёт производной проекции по dependencies.
-- Отличие: Источник раскрывается в immutable capture; историю аудита копирует Cedrus, производные Marts считает Spark.
-- Конец контекста миграции.
-- Сохраняет соответствие каждой Kafka-позиции исходной и объединённой партиям.
-- Engine: trino. Столбцы INSERT берутся в порядке DDL, не по позиции исходного SELECT.
-- INITIAL: согласованный исходный образ C0; исторический аудит не пересоздаётся.
SELECT topic,
       kafka_partition,
       kafka_offset,
       original_batch_id,
       coalesced_batch_id
FROM {{source:oil_stg__batch_coalesce_event_map}};
