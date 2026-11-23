-- Контекст миграции: AS IS / TO BE.
-- Назначение: Сохраняет JSON доставок с topic, partition, offset и исходной партией.
-- AS IS: oil_stg.event_log в GP; выбранная исходная бизнес-сущность.
-- TO BE: Cedrus исполняет только проверки текущей phase и both; публикация разрешена при нулевых нарушениях.
-- Отличие: PK/FK/UNIQUE Iceberg не обеспечивает; SQL проверяет grain, дубли, ссылки, SCD2 и равенство на C0/C1.
-- Конец контекста миграции.
-- Проверка: initial_full_equality; phase=initial
-- INITIAL: полное мультимножественное равенство сырья и исходных audit-полей на закреплённой границе.
-- Единственный результат n обязан быть 0; иначе публикация блокируется.
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

-- Проверка: raw_coordinates; phase=both
-- INITIAL/REFRESH: обязательные координаты и документ; одна строка на позицию Kafka.
-- Единственный результат n обязан быть 0; иначе публикация блокируется.
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

-- Проверка: refresh_position_conflicts; phase=both
-- REFRESH: повтор позиции Kafka с другим исходным JSON запрещает публикацию.
-- Единственный результат n обязан быть 0; иначе публикация блокируется.
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

-- Проверка: refresh_delta_coverage; phase=both
-- REFRESH: каждая доставка delta представлена в результате той же позицией и тем же документом.
-- Единственный результат n обязан быть 0; иначе публикация блокируется.
SELECT count(*) AS n
FROM {{delta}} d
WHERE NOT EXISTS
        (SELECT 1
         FROM {{target:oil_stg__event_log}} t
         WHERE t.topic=d.topic
             AND t.kafka_partition=d.kafka_partition
             AND t.kafka_offset=d.kafka_offset
             AND t.event_json=d.event_json);
