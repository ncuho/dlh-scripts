-- Контекст миграции: AS IS / TO BE.
-- Назначение: Сохраняет происхождение доставки после объединения партий.
-- AS IS: oil_stg.batch_coalesce_event_map в GP; выбранная исходная бизнес-сущность.
-- TO BE: Cedrus исполняет только проверки текущей phase и both; публикация разрешена при нулевых нарушениях.
-- Отличие: PK/FK/UNIQUE Iceberg не обеспечивает; SQL проверяет grain, дубли, ссылки, SCD2 и равенство на C0/C1.
-- Конец контекста миграции.
-- Проверка: initial_full_equality; phase=initial
-- INITIAL: сохранить полный исходный аудит сопоставления событий и партий.
-- Единственный результат n обязан быть 0; иначе публикация блокируется.
WITH s AS
    (SELECT topic,
            kafka_partition,
            kafka_offset,
            original_batch_id,
            coalesced_batch_id
     FROM {{source:oil_stg__batch_coalesce_event_map}}),
     t AS
    (SELECT topic,
            kafka_partition,
            kafka_offset,
            original_batch_id,
            coalesced_batch_id
     FROM {{target:oil_stg__batch_coalesce_event_map}}),
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
