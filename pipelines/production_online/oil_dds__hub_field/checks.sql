-- Проверка: hub_field_initial_exact; phase=initial
-- Полное равенство исходному hub вместе с аудитом.
-- Единственный результат n обязан быть 0; иначе публикация блокируется.
SELECT count(*) AS n
FROM (
          (SELECT field_hk,
                  field_id,
                  load_dts,
                  record_source
           FROM {{source:oil_dds__hub_field}}
           EXCEPT ALL SELECT field_hk,
                             field_id,
                             load_dts,
                             record_source
           FROM {{target:oil_dds__hub_field}})
      UNION ALL
          (SELECT field_hk,
                  field_id,
                  load_dts,
                  record_source
           FROM {{target:oil_dds__hub_field}}
           EXCEPT ALL SELECT field_hk,
                             field_id,
                             load_dts,
                             record_source
           FROM {{source:oil_dds__hub_field}})) q;

-- Проверка: hub_field_initial_retained; phase=refresh
-- Существующие ключи сохраняют исходные audit-поля.
-- Единственный результат n обязан быть 0; иначе публикация блокируется.
SELECT count(*) AS n
FROM
    (SELECT field_hk,
            field_id,
            load_dts,
            record_source
     FROM {{initial:oil_dds__hub_field}}
     EXCEPT ALL SELECT field_hk,
                       field_id,
                       load_dts,
                       record_source
     FROM {{target:oil_dds__hub_field}}) q;

-- Проверка: hub_field_business; phase=both
-- Ключи полностью соответствуют field-истории и исходной формуле md5.
-- Единственный результат n обязан быть 0; иначе публикация блокируется.
WITH expected AS
    (SELECT DISTINCT lower(to_hex(md5(to_utf8('field:' || entity_id)))) AS field_hk,
                     entity_id AS field_id
     FROM {{target:oil_dds__event}}
     WHERE event_type='field'),
     actual AS
    (SELECT field_hk,
            field_id
     FROM {{target:oil_dds__hub_field}})
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

-- Проверка: hub_field_unique; phase=both
-- Уникальность хеша и бизнес-ключа, отсутствие NULL в обязательных полях.
-- Единственный результат n обязан быть 0; иначе публикация блокируется.
SELECT count(*) AS n
FROM
    (SELECT field_hk AS k
     FROM {{target:oil_dds__hub_field}}
     GROUP BY field_hk
     HAVING field_hk IS NULL
     OR count(*)>1
     UNION ALL SELECT field_id AS k
     FROM {{target:oil_dds__hub_field}}
     GROUP BY field_id
     HAVING field_id IS NULL
     OR count(*)>1
     UNION ALL SELECT field_hk AS k
     FROM {{target:oil_dds__hub_field}}
     WHERE load_dts IS NULL
         OR record_source IS NULL) q;
