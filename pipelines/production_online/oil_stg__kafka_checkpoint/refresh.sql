-- Хранит следующий принятый offset каждой исходной Kafka-партиции. Здесь сохраняется аудит GP на C0, а не checkpoint нового sink.
SELECT topic,
       kafka_partition,
       next_offset,
       updated_at
FROM {{initial:oil_stg__kafka_checkpoint}};
