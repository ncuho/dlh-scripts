-- Контекст миграции: AS IS / TO BE.
-- Назначение: Хранит версии атрибутов well, происхождение, порядок версий и операции изменения.
-- AS IS: oil_dds.sat_well в GP; выбранная исходная бизнес-сущность.
-- TO BE: Cedrus исполняет только проверки текущей phase и both; публикация разрешена при нулевых нарушениях.
-- Отличие: PK/FK/UNIQUE Iceberg не обеспечивает; SQL проверяет grain, дубли, ссылки, SCD2 и равенство на C0/C1.
-- Конец контекста миграции.
-- Проверка: sat_well_initial_exact; phase=initial
-- Точная исходная копия всех версий well, включая дату, hashdiff и аудит.
-- Единственный результат n обязан быть 0; иначе публикация блокируется.
SELECT count(*) AS n
FROM (
          (SELECT well_hk,
                  link_hk,
                  event_id,
                  entity_version,
                  op,
                  event_time,
                  load_dts,
                  record_source,
                  hashdiff,
                  well_name,
                  valid_from,
                  status
           FROM {{source:oil_dds__sat_well}}
           EXCEPT ALL SELECT well_hk,
                             link_hk,
                             event_id,
                             entity_version,
                             op,
                             event_time,
                             load_dts,
                             record_source,
                             hashdiff,
                             well_name,
                             valid_from,
                             status
           FROM {{target:oil_dds__sat_well}})
      UNION ALL
          (SELECT well_hk,
                  link_hk,
                  event_id,
                  entity_version,
                  op,
                  event_time,
                  load_dts,
                  record_source,
                  hashdiff,
                  well_name,
                  valid_from,
                  status
           FROM {{target:oil_dds__sat_well}}
           EXCEPT ALL SELECT well_hk,
                             link_hk,
                             event_id,
                             entity_version,
                             op,
                             event_time,
                             load_dts,
                             record_source,
                             hashdiff,
                             well_name,
                             valid_from,
                             status
           FROM {{source:oil_dds__sat_well}})) q;

-- Проверка: sat_well_initial_retained; phase=refresh
-- Исходные версии и их аудит остаются неизменными.
-- Единственный результат n обязан быть 0; иначе публикация блокируется.
SELECT count(*) AS n
FROM
    (SELECT well_hk,
            link_hk,
            event_id,
            entity_version,
            op,
            event_time,
            load_dts,
            record_source,
            hashdiff,
            well_name,
            valid_from,
            status
     FROM {{initial:oil_dds__sat_well}}
     EXCEPT ALL SELECT well_hk,
                       link_hk,
                       event_id,
                       entity_version,
                       op,
                       event_time,
                       load_dts,
                       record_source,
                       hashdiff,
                       well_name,
                       valid_from,
                       status
     FROM {{target:oil_dds__sat_well}}) q;

-- Проверка: sat_well_business; phase=both
-- Полное соответствие well-истории, включая удаления, изменения отношений и точный hashdiff.
-- Единственный результат n обязан быть 0; иначе публикация блокируется.
WITH expected AS
    (SELECT lower(to_hex(md5(to_utf8('well:' || entity_id)))) AS well_hk,
            lower(to_hex(md5(to_utf8('well_field:' || entity_id || ':' || json_extract_scalar(payload, '$.field_id'))))) AS link_hk,
            event_id,
            entity_version,
            op,
            event_time,
            lower(to_hex(md5(to_utf8(op || ':' || payload)))) AS hashdiff,
            json_extract_scalar(payload, '$.well_name') AS well_name,
            try_cast(json_extract_scalar(payload, '$.valid_from') AS date) AS valid_from,
            json_extract_scalar(payload, '$.status') AS status
     FROM {{target:oil_dds__event}}
     WHERE event_type='well'),
     actual AS
    (SELECT well_hk,
            link_hk,
            event_id,
            entity_version,
            op,
            event_time,
            hashdiff,
            well_name,
            valid_from,
            status
     FROM {{target:oil_dds__sat_well}})
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

-- Проверка: sat_well_keys_and_references; phase=both
-- Уникальность событий и версий; ссылки hub/link/event; аудит спутника соответствует ledger.
-- Единственный результат n обязан быть 0; иначе публикация блокируется.
SELECT count(*) AS n
FROM
    (SELECT event_id AS k
     FROM {{target:oil_dds__sat_well}}
     GROUP BY event_id
     HAVING event_id IS NULL
     OR count(*)>1
     UNION ALL SELECT well_hk AS k
     FROM {{target:oil_dds__sat_well}}
     GROUP BY well_hk,
              entity_version
     HAVING well_hk IS NULL
     OR entity_version IS NULL
     OR count(*)>1
     UNION ALL SELECT s.event_id AS k
     FROM {{target:oil_dds__sat_well}} s
     LEFT JOIN {{target:oil_dds__hub_well}} h ON h.well_hk=s.well_hk
     LEFT JOIN {{target:oil_dds__link_well_field}} l ON l.link_hk=s.link_hk
     LEFT JOIN {{target:oil_dds__event}} e ON e.event_id=s.event_id
     WHERE h.well_hk IS NULL
         OR l.link_hk IS NULL
         OR l.well_hk IS DISTINCT
         FROM s.well_hk
         OR e.event_id IS NULL
         OR s.load_dts IS DISTINCT
         FROM e.load_dts
         OR s.record_source IS DISTINCT
         FROM e.record_source
         OR s.valid_from IS NULL
         OR nullif(s.well_name, '') IS NULL
         OR s.status IS NULL
         OR s.status NOT IN ('ACTIVE',
                             'INACTIVE')) q;
