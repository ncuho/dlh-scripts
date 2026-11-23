-- Контекст миграции: AS IS / TO BE.
-- Назначение: Учитывает состояния исходных партий и завершённость применения.
-- AS IS: oil_stg.batch_control в GP; выбранная исходная бизнес-сущность.
-- TO BE: INITIAL C0: перенос сохранённой исходной истории или расчёт производной проекции по dependencies.
-- Отличие: Источник раскрывается в immutable capture; историю аудита копирует Cedrus, производные Marts считает Spark.
-- Конец контекста миграции.
-- Хранит жизненный цикл партии: OPEN, READY, COALESCED, APPLIED или FAILED. Отделяет успешную доставку от применения в DDS.
-- Engine: trino. Столбцы INSERT берутся в порядке DDL, не по позиции исходного SELECT.
-- INITIAL: согласованный исходный образ C0; исторический аудит не пересоздаётся.
SELECT batch_id,
       created_at,
       error_text,
       source_manifest,
       source_name,
       status
FROM {{source:oil_stg__batch_control}};
