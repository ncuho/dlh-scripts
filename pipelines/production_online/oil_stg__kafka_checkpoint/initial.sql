-- Хранит следующий ожидаемый offset каждой партиции. Обновляется в одной транзакции с записью события в GP.
-- Engine: trino. Столбцы INSERT берутся в порядке DDL, не по позиции исходного SELECT.
-- INITIAL: согласованный исходный образ C0; исторический аудит не пересоздаётся.
SELECT topic,
       kafka_partition,
       next_offset,
       updated_at
FROM {{source:oil_stg__kafka_checkpoint}};
