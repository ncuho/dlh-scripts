-- Хранит следующий ожидаемый offset каждой партиции. Обновляется в одной транзакции с записью события в GP.
-- Engine: trino. Столбцы INSERT берутся в порядке DDL, не по позиции исходного SELECT.
-- REFRESH: закреплённый INITIAL + весь диапазон C0–C1; исключены более поздние offsets.
SELECT topic,
       kafka_partition,
       next_offset,
       updated_at
FROM {{initial:oil_stg__kafka_checkpoint}};
