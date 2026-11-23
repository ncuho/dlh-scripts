-- точное сохранение исходной границы и её аудита.
WITH s AS (SELECT topic,kafka_partition,next_offset,updated_at FROM {{source:oil_stg__kafka_checkpoint}}), t AS (SELECT topic,kafka_partition,next_offset,updated_at FROM {{target:oil_stg__kafka_checkpoint}}), d AS ((SELECT * FROM s EXCEPT ALL SELECT * FROM t) UNION ALL (SELECT * FROM t EXCEPT ALL SELECT * FROM s)) SELECT count(*) AS n FROM d;
