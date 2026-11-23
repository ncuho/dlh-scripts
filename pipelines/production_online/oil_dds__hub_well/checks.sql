-- Проверка: hub_well_initial_exact; phase=initial
-- Полное исходное равенство hub_well, включая аудит.
-- Единственный результат n обязан быть 0; иначе публикация блокируется.
SELECT count(*) AS n
FROM (
          (SELECT well_hk,
                  well_id,
                  load_dts,
                  record_source
           FROM {{source:oil_dds__hub_well}}
           EXCEPT ALL SELECT well_hk,
                             well_id,
                             load_dts,
                             record_source
           FROM {{target:oil_dds__hub_well}})
      UNION ALL
          (SELECT well_hk,
                  well_id,
                  load_dts,
                  record_source
           FROM {{target:oil_dds__hub_well}}
           EXCEPT ALL SELECT well_hk,
                             well_id,
                             load_dts,
                             record_source
           FROM {{source:oil_dds__hub_well}})) q;

-- Проверка: hub_well_initial_retained; phase=refresh
-- Неизменность первоначальных ключей и аудита.
-- Единственный результат n обязан быть 0; иначе публикация блокируется.
SELECT count(*) AS n
FROM
    (SELECT well_hk,
            well_id,
            load_dts,
            record_source
     FROM {{initial:oil_dds__hub_well}}
     EXCEPT ALL SELECT well_hk,
                       well_id,
                       load_dts,
                       record_source
     FROM {{target:oil_dds__hub_well}}) q;

-- Проверка: hub_well_business; phase=both
-- Полнота ключей well и исходная формула md5('well:' || entity_id).
-- Единственный результат n обязан быть 0; иначе публикация блокируется.
WITH expected AS
    (SELECT DISTINCT lower(to_hex(md5(to_utf8('well:' || entity_id)))) AS well_hk,
                     entity_id AS well_id
     FROM {{target:oil_dds__event}}
     WHERE event_type='well'),
     actual AS
    (SELECT well_hk,
            well_id
     FROM {{target:oil_dds__hub_well}})
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

-- Проверка: hub_well_unique; phase=both
-- Уникальность бизнес-ключа и хеша; обязательные audit-поля.
-- Единственный результат n обязан быть 0; иначе публикация блокируется.
SELECT count(*) AS n
FROM
    (SELECT well_hk AS k
     FROM {{target:oil_dds__hub_well}}
     GROUP BY well_hk
     HAVING well_hk IS NULL
     OR count(*)>1
     UNION ALL SELECT well_id AS k
     FROM {{target:oil_dds__hub_well}}
     GROUP BY well_id
     HAVING well_id IS NULL
     OR count(*)>1
     UNION ALL SELECT well_hk AS k
     FROM {{target:oil_dds__hub_well}}
     WHERE load_dts IS NULL
         OR record_source IS NULL) q;
