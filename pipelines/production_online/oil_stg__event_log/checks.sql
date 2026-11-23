-- Полное мультимножественное равенство сырья и исходных audit-полей на закреплённой границе.
WITH s AS
    (SELECT topic,
            kafka_partition,
            kafka_offset,
            batch_id,
            received_at,
            event_json
     FROM {{source:oil_stg__event_log}}),
     t AS
    (SELECT topic,
            kafka_partition,
            kafka_offset,
            batch_id,
            received_at,
            event_json
     FROM {{target:oil_stg__event_log}}),
     d AS (
               (SELECT *
                FROM s
                EXCEPT ALL SELECT *
                FROM t)
           UNION ALL
               (SELECT *
                FROM t
                EXCEPT ALL SELECT *
                FROM s))
SELECT count(*) AS n
FROM d;


-- Обязательные координаты и документ; одна строка на позицию Kafka.
SELECT count(*) AS n
FROM
    (SELECT topic,
            kafka_partition,
            kafka_offset
     FROM {{target:oil_stg__event_log}}
     GROUP BY topic,
              kafka_partition,
              kafka_offset
     HAVING count(*)<>1
     OR topic IS NULL
     OR kafka_partition IS NULL
     OR kafka_offset IS NULL
     UNION ALL SELECT topic,
                      kafka_partition,
                      kafka_offset
     FROM {{target:oil_stg__event_log}}
     WHERE event_json IS NULL
         OR received_at IS NULL
         OR batch_id IS NULL) q;


-- Повтор позиции Kafka с другим исходным JSON запрещает публикацию.
WITH r AS
    (SELECT topic,
            kafka_partition,
            kafka_offset,
            event_json
     FROM {{target:oil_stg__event_log}}
     UNION ALL SELECT topic,
                      kafka_partition,
                      kafka_offset,
                      event_json
     FROM {{delta}})
SELECT count(*) AS n
FROM
    (SELECT topic,
            kafka_partition,
            kafka_offset
     FROM r
     GROUP BY topic,
              kafka_partition,
              kafka_offset
     HAVING count(DISTINCT event_json)>1
     OR count(event_json)<>count(*)) q;


-- Каждая доставка delta представлена в результате той же позицией и тем же документом.
SELECT count(*) AS n
FROM {{delta}} d
WHERE NOT EXISTS
        (SELECT 1
         FROM {{target:oil_stg__event_log}} t
         WHERE t.topic=d.topic
             AND t.kafka_partition=d.kafka_partition
             AND t.kafka_offset=d.kafka_offset
             AND t.event_json=d.event_json);
