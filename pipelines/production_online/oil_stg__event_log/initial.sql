-- Контекст миграции: AS IS / TO BE.
-- Назначение: Сохраняет JSON доставок с topic, partition, offset и исходной партией.
-- AS IS: oil_stg.event_log в GP; выбранная исходная бизнес-сущность.
-- TO BE: INITIAL C0: перенос сохранённой исходной истории или расчёт производной проекции по dependencies.
-- Отличие: Источник раскрывается в immutable capture; историю аудита копирует Cedrus, производные Marts считает Spark.
-- Конец контекста миграции.
-- Принимает исходные JSON-события вместе с topic, partition, offset и ID партии. Сохраняет доставку до применения бизнес-логики.
-- Engine: trino. Столбцы INSERT берутся в порядке DDL, не по позиции исходного SELECT.
-- INITIAL: согласованный исходный образ C0; исторический аудит не пересоздаётся.
SELECT topic,
       kafka_partition,
       kafka_offset,
       batch_id,
       received_at,
       event_json
FROM {{source:oil_stg__event_log}};
