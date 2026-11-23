-- Контекст миграции: AS IS / TO BE.
-- Назначение: Учитывает состояния исходных партий и завершённость применения.
-- AS IS: oil_stg.batch_control в GP; выбранная исходная бизнес-сущность.
-- TO BE: Cedrus исполняет только проверки текущей phase и both; публикация разрешена при нулевых нарушениях.
-- Отличие: PK/FK/UNIQUE Iceberg не обеспечивает; SQL проверяет grain, дубли, ссылки, SCD2 и равенство на C0/C1.
-- Конец контекста миграции.
-- Проверка: initial_full_equality; phase=initial
-- INITIAL: сохранить статусы, manifests и остальные исходные audit-поля.
-- Единственный результат n обязан быть 0; иначе публикация блокируется.
WITH s AS
    (SELECT batch_id,
            created_at,
            error_text,
            source_manifest,
            source_name,
            status
     FROM {{source:oil_stg__batch_control}}),
     t AS
    (SELECT batch_id,
            created_at,
            error_text,
            source_manifest,
            source_name,
            status
     FROM {{target:oil_stg__batch_control}}),
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
