-- Контекст миграции: AS IS / TO BE.
-- Назначение: Хранит следующий принятый offset каждой исходной Kafka-партиции.
-- AS IS: oil_stg.kafka_checkpoint в GP; выбранная исходная бизнес-сущность.
-- TO BE: INITIAL C0: перенос сохранённой исходной истории или расчёт производной проекции по dependencies.
-- Отличие: Источник раскрывается в immutable capture; историю аудита копирует Cedrus, производные Marts считает Spark.
-- Конец контекста миграции.
-- Хранит следующий ожидаемый offset каждой партиции. Обновляется в одной транзакции с записью события в GP.
-- Engine: trino. Столбцы INSERT берутся в порядке DDL, не по позиции исходного SELECT.
-- INITIAL: согласованный исходный образ C0; исторический аудит не пересоздаётся.
SELECT topic,
       kafka_partition,
       next_offset,
       updated_at
FROM {{source:oil_stg__kafka_checkpoint}};
