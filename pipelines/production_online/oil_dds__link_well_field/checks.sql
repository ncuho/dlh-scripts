-- Точная INITIAL-копия связи с исходным аудитом.
SELECT count(*) AS n
FROM (
          (SELECT link_hk,
                  well_hk,
                  field_hk,
                  load_dts,
                  record_source
           FROM {{source:oil_dds__link_well_field}}
           EXCEPT ALL SELECT link_hk,
                             well_hk,
                             field_hk,
                             load_dts,
                             record_source
           FROM {{target:oil_dds__link_well_field}})
      UNION ALL
          (SELECT link_hk,
                  well_hk,
                  field_hk,
                  load_dts,
                  record_source
           FROM {{target:oil_dds__link_well_field}}
           EXCEPT ALL SELECT link_hk,
                             well_hk,
                             field_hk,
                             load_dts,
                             record_source
           FROM {{source:oil_dds__link_well_field}})) q;


-- Исходные отношения и аудит сохранены.
SELECT count(*) AS n
FROM
    (SELECT link_hk,
            well_hk,
            field_hk,
            load_dts,
            record_source
     FROM {{initial:oil_dds__link_well_field}}
     EXCEPT ALL SELECT link_hk,
                       well_hk,
                       field_hk,
                       load_dts,
                       record_source
     FROM {{target:oil_dds__link_well_field}}) q;


-- Проверка всех исторических отношений well—field и исходных формул ключей.
WITH expected AS
    (SELECT DISTINCT lower(to_hex(md5(to_utf8('well_field:' || entity_id || ':' || json_extract_scalar(payload, '$.field_id'))))) AS link_hk,
                     lower(to_hex(md5(to_utf8('well:' || entity_id)))) AS well_hk,
                     lower(to_hex(md5(to_utf8('field:' || json_extract_scalar(payload, '$.field_id'))))) AS field_hk
     FROM {{target:oil_dds__event}}
     WHERE event_type='well'),
     actual AS
    (SELECT link_hk,
            well_hk,
            field_hk
     FROM {{target:oil_dds__link_well_field}})
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


-- Уникальность hash/pair и наличие обоих родителей.
SELECT count(*) AS n
FROM
    (SELECT link_hk AS k
     FROM {{target:oil_dds__link_well_field}}
     GROUP BY link_hk
     HAVING link_hk IS NULL
     OR count(*)>1
     UNION ALL SELECT min(link_hk) AS k
     FROM {{target:oil_dds__link_well_field}}
     GROUP BY well_hk,
              field_hk
     HAVING count(*)>1
     UNION ALL SELECT l.link_hk AS k
     FROM {{target:oil_dds__link_well_field}} l
     LEFT JOIN {{target:oil_dds__hub_well}} w ON w.well_hk=l.well_hk
     LEFT JOIN {{target:oil_dds__hub_field}} f ON f.field_hk=l.field_hk
     WHERE w.well_hk IS NULL
         OR f.field_hk IS NULL) q;
