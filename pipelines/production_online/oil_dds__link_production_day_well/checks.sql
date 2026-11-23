-- Проверка: link_production_well_initial_exact; phase=initial
-- Точная исходная копия всех столбцов связи, включая аудит.
-- Единственный результат n обязан быть 0; иначе публикация блокируется.
SELECT count(*) AS n
FROM (
          (SELECT link_hk,
                  production_hk,
                  well_hk,
                  load_dts,
                  record_source
           FROM {{source:oil_dds__link_production_day_well}}
           EXCEPT ALL SELECT link_hk,
                             production_hk,
                             well_hk,
                             load_dts,
                             record_source
           FROM {{target:oil_dds__link_production_day_well}})
      UNION ALL
          (SELECT link_hk,
                  production_hk,
                  well_hk,
                  load_dts,
                  record_source
           FROM {{target:oil_dds__link_production_day_well}}
           EXCEPT ALL SELECT link_hk,
                             production_hk,
                             well_hk,
                             load_dts,
                             record_source
           FROM {{source:oil_dds__link_production_day_well}})) q;

-- Проверка: link_production_well_initial_retained; phase=refresh
-- Существующие связи и их исходный аудит не изменены.
-- Единственный результат n обязан быть 0; иначе публикация блокируется.
SELECT count(*) AS n
FROM
    (SELECT link_hk,
            production_hk,
            well_hk,
            load_dts,
            record_source
     FROM {{initial:oil_dds__link_production_day_well}}
     EXCEPT ALL SELECT link_hk,
                       production_hk,
                       well_hk,
                       load_dts,
                       record_source
     FROM {{target:oil_dds__link_production_day_well}}) q;

-- Проверка: link_production_well_business; phase=both
-- Все исторические отношения production_day—well сохранены с исходными формулами ключей.
-- Единственный результат n обязан быть 0; иначе публикация блокируется.
WITH expected AS
    (SELECT DISTINCT lower(to_hex(md5(to_utf8('production_day_well:' || entity_id || ':' || json_extract_scalar(payload, '$.well_id'))))) AS link_hk,
                     lower(to_hex(md5(to_utf8('production:' || entity_id)))) AS production_hk,
                     lower(to_hex(md5(to_utf8('well:' || json_extract_scalar(payload, '$.well_id'))))) AS well_hk
     FROM {{target:oil_dds__event}}
     WHERE event_type='production_day'),
     actual AS
    (SELECT link_hk,
            production_hk,
            well_hk
     FROM {{target:oil_dds__link_production_day_well}})
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

-- Проверка: link_production_well_keys_and_references; phase=both
-- Уникальность ключа и пары родителей; отсутствие потерянных ссылок.
-- Единственный результат n обязан быть 0; иначе публикация блокируется.
SELECT count(*) AS n
FROM
    (SELECT link_hk AS k
     FROM {{target:oil_dds__link_production_day_well}}
     GROUP BY link_hk
     HAVING link_hk IS NULL
     OR count(*)>1
     UNION ALL SELECT min(link_hk) AS k
     FROM {{target:oil_dds__link_production_day_well}}
     GROUP BY production_hk,
              well_hk
     HAVING count(*)>1
     UNION ALL SELECT l.link_hk AS k
     FROM {{target:oil_dds__link_production_day_well}} l
     LEFT JOIN {{target:oil_dds__hub_production_day}} p ON p.production_hk=l.production_hk
     LEFT JOIN {{target:oil_dds__hub_well}} w ON w.well_hk=l.well_hk
     WHERE p.production_hk IS NULL
         OR w.well_hk IS NULL) q;
