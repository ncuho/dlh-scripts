-- Контекст миграции: AS IS / TO BE.
-- Назначение: Хранит следующий принятый offset каждой исходной Kafka-партиции.
-- AS IS: oil_stg.kafka_checkpoint в GP; выбранная исходная бизнес-сущность.
-- TO BE: REFRESH: отдельный выпуск из pinned INITIAL и полного raw-окна [C0,C1).
-- Отличие: Сохраняется аудит GP на C0; это не новый checkpoint Iceberg consumer и не live source C1.
-- Конец контекста миграции.
-- Хранит следующий ожидаемый offset каждой партиции. Обновляется в одной транзакции с записью события в GP.
-- Engine: trino. Столбцы INSERT берутся в порядке DDL, не по позиции исходного SELECT.
-- REFRESH: закреплённый INITIAL + весь диапазон C0–C1; исключены более поздние offsets.
SELECT topic,
       kafka_partition,
       next_offset,
       updated_at
FROM {{initial:oil_stg__kafka_checkpoint}};
