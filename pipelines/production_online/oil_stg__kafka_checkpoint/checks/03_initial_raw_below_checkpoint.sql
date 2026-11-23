-- у каждой сырой позиции есть checkpoint, и позиция строго меньше next_offset; это не доказывает полноту истории.
SELECT count(*) AS n FROM {{target:oil_stg__event_log}} r WHERE NOT EXISTS (SELECT 1 FROM {{target:oil_stg__kafka_checkpoint}} c WHERE c.topic=r.topic AND c.kafka_partition=r.kafka_partition AND r.kafka_offset<c.next_offset);
