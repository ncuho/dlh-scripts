-- Контекст миграции: AS IS / TO BE.
-- Назначение: Хранит версии атрибутов field, происхождение, порядок версий и операции изменения.
-- AS IS: oil_dds.sat_field в GP; выбранная исходная бизнес-сущность.
-- TO BE: Cedrus исполняет только проверки текущей phase и both; публикация разрешена при нулевых нарушениях.
-- Отличие: PK/FK/UNIQUE Iceberg не обеспечивает; SQL проверяет grain, дубли, ссылки, SCD2 и равенство на C0/C1.
-- Конец контекста миграции.
-- Проверка: sat_field_initial_exact; phase=initial
-- Полное равенство исходной копии, включая аудит и hashdiff; ожидается n=0.
-- Единственный результат n обязан быть 0; иначе публикация блокируется.
SELECT count(*) AS n
FROM (
          (SELECT field_hk,
                  event_id,
                  entity_version,
                  op,
                  event_time,
                  load_dts,
                  record_source,
                  hashdiff,
                  field_name,
                  region
           FROM {{source:oil_dds__sat_field}}
           EXCEPT ALL SELECT field_hk,
                             event_id,
                             entity_version,
                             op,
                             event_time,
                             load_dts,
                             record_source,
                             hashdiff,
                             field_name,
                             region
           FROM {{target:oil_dds__sat_field}})
      UNION ALL
          (SELECT field_hk,
                  event_id,
                  entity_version,
                  op,
                  event_time,
                  load_dts,
                  record_source,
                  hashdiff,
                  field_name,
                  region
           FROM {{target:oil_dds__sat_field}}
           EXCEPT ALL SELECT field_hk,
                             event_id,
                             entity_version,
                             op,
                             event_time,
                             load_dts,
                             record_source,
                             hashdiff,
                             field_name,
                             region
           FROM {{source:oil_dds__sat_field}})) q;

-- Проверка: sat_field_initial_retained; phase=refresh
-- Все исходные версии и их аудит сохранены без изменений.
-- Единственный результат n обязан быть 0; иначе публикация блокируется.
SELECT count(*) AS n
FROM
    (SELECT field_hk,
            event_id,
            entity_version,
            op,
            event_time,
            load_dts,
            record_source,
            hashdiff,
            field_name,
            region
     FROM {{initial:oil_dds__sat_field}}
     EXCEPT ALL SELECT field_hk,
                       event_id,
                       entity_version,
                       op,
                       event_time,
                       load_dts,
                       record_source,
                       hashdiff,
                       field_name,
                       region
     FROM {{target:oil_dds__sat_field}}) q;

-- Проверка: sat_field_business; phase=both
-- Полное соответствие истории field реестру событий; исходная формула hashdiff применяется к сохранённому тексту payload.
-- Единственный результат n обязан быть 0; иначе публикация блокируется.
WITH expected AS
    (SELECT lower(to_hex(md5(to_utf8('field:' || entity_id)))) AS field_hk,
            event_id,
            entity_version,
            op,
            event_time,
            lower(to_hex(md5(to_utf8(op || ':' || payload)))) AS hashdiff,
            json_extract_scalar(payload, '$.field_name') AS field_name,
            json_extract_scalar(payload, '$.region') AS region
     FROM {{target:oil_dds__event}}
     WHERE event_type='field'),
     actual AS
    (SELECT field_hk,
            event_id,
            entity_version,
            op,
            event_time,
            hashdiff,
            field_name,
            region
     FROM {{target:oil_dds__sat_field}})
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

-- Проверка: sat_field_keys_and_references; phase=both
-- Уникальность event_id и версии объекта; наличие родительского hub и события с тем же аудитом.
-- Единственный результат n обязан быть 0; иначе публикация блокируется.
SELECT count(*) AS n
FROM
    (SELECT event_id AS k
     FROM {{target:oil_dds__sat_field}}
     GROUP BY event_id
     HAVING event_id IS NULL
     OR count(*)>1
     UNION ALL SELECT field_hk AS k
     FROM {{target:oil_dds__sat_field}}
     GROUP BY field_hk,
              entity_version
     HAVING field_hk IS NULL
     OR entity_version IS NULL
     OR count(*)>1
     UNION ALL SELECT s.event_id AS k
     FROM {{target:oil_dds__sat_field}} s
     LEFT JOIN {{target:oil_dds__hub_field}} h ON h.field_hk=s.field_hk
     LEFT JOIN {{target:oil_dds__event}} e ON e.event_id=s.event_id
     WHERE h.field_hk IS NULL
         OR e.event_id IS NULL
         OR s.load_dts IS DISTINCT
         FROM e.load_dts
         OR s.record_source IS DISTINCT
         FROM e.record_source) q;
