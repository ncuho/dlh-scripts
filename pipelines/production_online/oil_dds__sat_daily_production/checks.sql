-- Полное исходное равенство, включая decimal, даты, hashdiff и аудит.
SELECT count(*) AS n
FROM (
          (SELECT production_hk,
                  link_hk,
                  event_id,
                  entity_version,
                  op,
                  event_time,
                  load_dts,
                  record_source,
                  hashdiff,
                  production_date,
                  oil_tonnes,
                  status
           FROM {{source:oil_dds__sat_daily_production}}
           EXCEPT ALL SELECT production_hk,
                             link_hk,
                             event_id,
                             entity_version,
                             op,
                             event_time,
                             load_dts,
                             record_source,
                             hashdiff,
                             production_date,
                             oil_tonnes,
                             status
           FROM {{target:oil_dds__sat_daily_production}})
      UNION ALL
          (SELECT production_hk,
                  link_hk,
                  event_id,
                  entity_version,
                  op,
                  event_time,
                  load_dts,
                  record_source,
                  hashdiff,
                  production_date,
                  oil_tonnes,
                  status
           FROM {{target:oil_dds__sat_daily_production}}
           EXCEPT ALL SELECT production_hk,
                             link_hk,
                             event_id,
                             entity_version,
                             op,
                             event_time,
                             load_dts,
                             record_source,
                             hashdiff,
                             production_date,
                             oil_tonnes,
                             status
           FROM {{source:oil_dds__sat_daily_production}})) q;


-- Сохранение всех первоначальных версий и аудита.
SELECT count(*) AS n
FROM
    (SELECT production_hk,
            link_hk,
            event_id,
            entity_version,
            op,
            event_time,
            load_dts,
            record_source,
            hashdiff,
            production_date,
            oil_tonnes,
            status
     FROM {{initial:oil_dds__sat_daily_production}}
     EXCEPT ALL SELECT production_hk,
                       link_hk,
                       event_id,
                       entity_version,
                       op,
                       event_time,
                       load_dts,
                       record_source,
                       hashdiff,
                       production_date,
                       oil_tonnes,
                       status
     FROM {{target:oil_dds__sat_daily_production}}) q;


-- Полная история production_day, включая D; независимая проверка ключей, hashdiff, даты и numeric(18,3).
WITH expected AS
    (SELECT lower(to_hex(md5(to_utf8('production:' || entity_id)))) AS production_hk,
            lower(to_hex(md5(to_utf8('production_day_well:' || entity_id || ':' || json_extract_scalar(payload, '$.well_id'))))) AS link_hk,
            event_id,
            entity_version,
            op,
            event_time,
            lower(to_hex(md5(to_utf8(op || ':' || payload)))) AS hashdiff,
            try_cast(json_extract_scalar(payload, '$.production_date') AS date) AS production_date,
            try_cast(json_extract_scalar(payload, '$.oil_tonnes') AS decimal(18, 3)) AS oil_tonnes,
            json_extract_scalar(payload, '$.status') AS status
     FROM {{target:oil_dds__event}}
     WHERE event_type='production_day'),
     actual AS
    (SELECT production_hk,
            link_hk,
            event_id,
            entity_version,
            op,
            event_time,
            hashdiff,
            production_date,
            oil_tonnes,
            status
     FROM {{target:oil_dds__sat_daily_production}})
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


-- Уникальность событий и версий, связи с hub/link/event, ненулевые результаты обязательных преобразований.
SELECT count(*) AS n
FROM
    (SELECT event_id AS k
     FROM {{target:oil_dds__sat_daily_production}}
     GROUP BY event_id
     HAVING event_id IS NULL
     OR count(*)>1
     UNION ALL SELECT production_hk AS k
     FROM {{target:oil_dds__sat_daily_production}}
     GROUP BY production_hk,
              entity_version
     HAVING production_hk IS NULL
     OR entity_version IS NULL
     OR count(*)>1
     UNION ALL SELECT s.event_id AS k
     FROM {{target:oil_dds__sat_daily_production}} s
     LEFT JOIN {{target:oil_dds__hub_production_day}} h ON h.production_hk=s.production_hk
     LEFT JOIN {{target:oil_dds__link_production_day_well}} l ON l.link_hk=s.link_hk
     LEFT JOIN {{target:oil_dds__event}} e ON e.event_id=s.event_id
     WHERE h.production_hk IS NULL
         OR l.link_hk IS NULL
         OR l.production_hk IS DISTINCT
         FROM s.production_hk
         OR e.event_id IS NULL
         OR s.load_dts IS DISTINCT
         FROM e.load_dts
         OR s.record_source IS DISTINCT
         FROM e.record_source
         OR s.production_date IS NULL
         OR s.oil_tonnes IS NULL
         OR s.oil_tonnes<0
         OR s.status IS NULL
         OR s.status NOT IN ('ACTIVE',
                             'INACTIVE')) q;
