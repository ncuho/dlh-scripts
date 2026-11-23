-- Контекст миграции: AS IS / TO BE.
-- Назначение: Хранит следующий принятый offset каждой исходной Kafka-партиции.
-- AS IS: oil_stg.kafka_checkpoint в GP; выбранная исходная бизнес-сущность.
-- TO BE: Cedrus исполняет только проверки текущей phase и both; публикация разрешена при нулевых нарушениях.
-- Отличие: PK/FK/UNIQUE Iceberg не обеспечивает; SQL проверяет grain, дубли, ссылки, SCD2 и равенство на C0/C1.
-- Конец контекста миграции.
-- Проверка: initial_full_equality; phase=initial
-- INITIAL: точное сохранение исходной границы и её аудита.
-- Единственный результат n обязан быть 0; иначе публикация блокируется.
WITH s AS
    (SELECT topic,
            kafka_partition,
            next_offset,
            updated_at
     FROM {{source:oil_stg__kafka_checkpoint}}),
     t AS
    (SELECT topic,
            kafka_partition,
            next_offset,
            updated_at
     FROM {{target:oil_stg__kafka_checkpoint}}),
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

-- Проверка: initial_checkpoint_coordinates; phase=initial
-- INITIAL: обязательность и единственность границы для каждой представленной партиции.
-- Единственный результат n обязан быть 0; иначе публикация блокируется.
SELECT count(*) AS n
FROM
    (SELECT topic,
            kafka_partition
     FROM {{target:oil_stg__kafka_checkpoint}}
     GROUP BY topic,
              kafka_partition
     HAVING count(*)<>1
     OR topic IS NULL
     OR kafka_partition IS NULL
     UNION ALL SELECT topic,
                      kafka_partition
     FROM {{target:oil_stg__kafka_checkpoint}}
     WHERE next_offset IS NULL
         OR updated_at IS NULL) q;

-- Проверка: initial_raw_below_checkpoint; phase=initial
-- INITIAL: у каждой сырой позиции есть checkpoint, и позиция строго меньше next_offset; это не доказывает полноту истории.
-- Единственный результат n обязан быть 0; иначе публикация блокируется.
SELECT count(*) AS n
FROM {{target:oil_stg__event_log}} r
WHERE NOT EXISTS
        (SELECT 1
         FROM {{target:oil_stg__kafka_checkpoint}} c
         WHERE c.topic=r.topic
             AND c.kafka_partition=r.kafka_partition
             AND r.kafka_offset<c.next_offset);
