-- Контекст миграции: AS IS / TO BE.
-- Назначение: Хранит устойчивый бизнес-ключ и hash key сущности production_day; атрибуты вынесены в satellites.
-- AS IS: oil_dds.hub_production_day в GP; выбранная исходная бизнес-сущность.
-- TO BE: Cedrus исполняет только проверки текущей phase и both; публикация разрешена при нулевых нарушениях.
-- Отличие: PK/FK/UNIQUE Iceberg не обеспечивает; SQL проверяет grain, дубли, ссылки, SCD2 и равенство на C0/C1.
-- Конец контекста миграции.
-- Проверка: hub_production_initial_exact; phase=initial
-- Точная INITIAL-копия всех исходных полей hub_production_day.
-- Единственный результат n обязан быть 0; иначе публикация блокируется.
SELECT count(*) AS n
FROM (
          (SELECT production_hk,
                  production_id,
                  load_dts,
                  record_source
           FROM {{source:oil_dds__hub_production_day}}
           EXCEPT ALL SELECT production_hk,
                             production_id,
                             load_dts,
                             record_source
           FROM {{target:oil_dds__hub_production_day}})
      UNION ALL
          (SELECT production_hk,
                  production_id,
                  load_dts,
                  record_source
           FROM {{target:oil_dds__hub_production_day}}
           EXCEPT ALL SELECT production_hk,
                             production_id,
                             load_dts,
                             record_source
           FROM {{source:oil_dds__hub_production_day}})) q;

-- Проверка: hub_production_initial_retained; phase=refresh
-- Сохранение исходных ключей и аудита при REFRESH.
-- Единственный результат n обязан быть 0; иначе публикация блокируется.
SELECT count(*) AS n
FROM
    (SELECT production_hk,
            production_id,
            load_dts,
            record_source
     FROM {{initial:oil_dds__hub_production_day}}
     EXCEPT ALL SELECT production_hk,
                       production_id,
                       load_dts,
                       record_source
     FROM {{target:oil_dds__hub_production_day}}) q;

-- Проверка: hub_production_business; phase=both
-- Полнота production_day-ключей и исходный префикс production:.
-- Единственный результат n обязан быть 0; иначе публикация блокируется.
WITH expected AS
    (SELECT DISTINCT lower(to_hex(md5(to_utf8('production:' || entity_id)))) AS production_hk,
                     entity_id AS production_id
     FROM {{target:oil_dds__event}}
     WHERE event_type='production_day'),
     actual AS
    (SELECT production_hk,
            production_id
     FROM {{target:oil_dds__hub_production_day}})
SELECT count(*) AS n
FROM (
          (SELECT *
           FROM expected
           EXCEPT ALL SELECT *
           FROM actual)
      UNION ALL
          (SELECT *
           FROM actual
           EXCEPT ALL SELECT *
           FROM expected)) q;

-- Проверка: hub_production_unique; phase=both
-- Уникальность хеша и production_id; обязательные audit-поля.
-- Единственный результат n обязан быть 0; иначе публикация блокируется.
SELECT count(*) AS n
FROM
    (SELECT production_hk AS k
     FROM {{target:oil_dds__hub_production_day}}
     GROUP BY production_hk
     HAVING production_hk IS NULL
     OR count(*)>1
     UNION ALL SELECT production_id AS k
     FROM {{target:oil_dds__hub_production_day}}
     GROUP BY production_id
     HAVING production_id IS NULL
     OR count(*)>1
     UNION ALL SELECT production_hk AS k
     FROM {{target:oil_dds__hub_production_day}}
     WHERE load_dts IS NULL
         OR record_source IS NULL) q;
