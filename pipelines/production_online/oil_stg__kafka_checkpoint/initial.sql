-- Хранит следующий принятый offset каждой исходной Kafka-партиции. История читается из C0; исходная GP-функция повторно не запускается.
SELECT topic,
       kafka_partition,
       next_offset,
       updated_at
FROM {{source:oil_stg__kafka_checkpoint}};
