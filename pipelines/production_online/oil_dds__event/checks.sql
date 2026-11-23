-- Проверка: event_initial_exact; phase=initial
-- Полное равенство исходному реестру: исходные GP batch_id, load_dts, record_source, payload и документ сохранены.
-- Единственный результат n обязан быть 0; иначе публикация блокируется.
SELECT count(*) AS n
FROM (
          (SELECT event_id,
                  event_type,
                  entity_id,
                  entity_version,
                  op,
                  event_time,
                  load_dts,
                  record_source,
                  batch_id,
                  payload,
                  event_document
           FROM {{source:oil_dds__event}}
           EXCEPT ALL SELECT event_id,
                             event_type,
                             entity_id,
                             entity_version,
                             op,
                             event_time,
                             load_dts,
                             record_source,
                             batch_id,
                             payload,
                             event_document
           FROM {{target:oil_dds__event}})
      UNION ALL
          (SELECT event_id,
                  event_type,
                  entity_id,
                  entity_version,
                  op,
                  event_time,
                  load_dts,
                  record_source,
                  batch_id,
                  payload,
                  event_document
           FROM {{target:oil_dds__event}}
           EXCEPT ALL SELECT event_id,
                             event_type,
                             entity_id,
                             entity_version,
                             op,
                             event_time,
                             load_dts,
                             record_source,
                             batch_id,
                             payload,
                             event_document
           FROM {{source:oil_dds__event}})) q;

-- Проверка: event_initial_retained; phase=refresh
-- Replay не меняет ни одного поля первоначально принятого события.
-- Единственный результат n обязан быть 0; иначе публикация блокируется.
SELECT count(*) AS n
FROM
    (SELECT event_id,
            event_type,
            entity_id,
            entity_version,
            op,
            event_time,
            load_dts,
            record_source,
            batch_id,
            payload,
            event_document
     FROM {{initial:oil_dds__event}}
     EXCEPT ALL SELECT event_id,
                       event_type,
                       entity_id,
                       entity_version,
                       op,
                       event_time,
                       load_dts,
                       record_source,
                       batch_id,
                       payload,
                       event_document
     FROM {{target:oil_dds__event}}) q;

-- Проверка: event_stage_business_coverage; phase=both
-- Каждая доставка всех четырёх типов отражена в ledger с теми же бизнес-полями, точным документом и payload. Batch ID доставки не сравнивается с исходным GP ID.
-- Единственный результат n обязан быть 0; иначе публикация блокируется.
SELECT count(*) AS n
FROM {{target:oil_stg__v_event_parsed}} p
LEFT JOIN {{target:oil_dds__event}} e ON e.event_id=p.event_id
WHERE e.event_id IS NULL
    OR e.event_type IS DISTINCT
    FROM p.event_type
    OR e.entity_id IS DISTINCT
    FROM p.entity_id
    OR e.entity_version IS DISTINCT
    FROM p.entity_version
    OR e.op IS DISTINCT
    FROM p.op
    OR e.event_time IS DISTINCT
    FROM p.event_time
    OR e.record_source IS DISTINCT
    FROM p.topic
    OR e.payload IS DISTINCT
    FROM p.payload
    OR e.event_document IS DISTINCT
    FROM p.event_json;

-- Проверка: event_id_and_version_conflicts; phase=both
-- Уникальность ledger; запрет повторного event_id с другим JSON и разных event_id для одной версии сущности.
-- Единственный результат n обязан быть 0; иначе публикация блокируется.
WITH history AS
    (SELECT event_id,
            event_type,
            entity_id,
            entity_version,
            event_document AS document
     FROM {{target:oil_dds__event}}
     UNION ALL SELECT event_id,
                      event_type,
                      entity_id,
                      entity_version,
                      event_json AS document
     FROM {{target:oil_stg__v_event_parsed}})
SELECT count(*) AS n
FROM
    (SELECT event_id AS k
     FROM {{target:oil_dds__event}}
     GROUP BY event_id
     HAVING event_id IS NULL
     OR count(*)>1
     UNION ALL SELECT event_id AS k
     FROM history
     GROUP BY event_id
     HAVING count(DISTINCT document)>1
     OR count_if(document IS NULL)>0
     UNION ALL SELECT min(event_id) AS k
     FROM history
     GROUP BY event_type,
              entity_id,
              entity_version
     HAVING event_type IS NULL
     OR entity_id IS NULL
     OR entity_version IS NULL
     OR count(DISTINCT event_id)>1) q;

-- Проверка: event_new_rows_audit_and_coverage; phase=refresh
-- Новые события имеют целевой cut_time и происходят из текущей доставки, выбранной детерминированно; batch_id является аудитом sink.
-- Единственный результат n обязан быть 0; иначе публикация блокируется.
WITH ranked AS
    (SELECT p.*,
            row_number() OVER (PARTITION BY event_id
                               ORDER BY topic, kafka_partition, kafka_offset, batch_id) AS rn
     FROM {{target:oil_stg__v_event_parsed}} p),
     fresh AS
    (SELECT e.*
     FROM {{target:oil_dds__event}} e
     WHERE NOT EXISTS
             (SELECT 1
              FROM {{initial:oil_dds__event}} i
              WHERE i.event_id=e.event_id))
SELECT count(*) AS n
FROM fresh e
LEFT JOIN ranked p ON p.event_id=e.event_id
AND p.rn=1
WHERE p.event_id IS NULL
    OR e.load_dts IS DISTINCT
    FROM {{cut_time}}
    OR e.record_source IS DISTINCT
    FROM p.topic
    OR e.batch_id IS DISTINCT
    FROM p.batch_id
    OR e.event_document IS DISTINCT
    FROM p.event_json;

-- Проверка: event_document_consistency; phase=both
-- Ledger содержит валидный JSON и согласованные поля envelope/payload, включая downtime.
-- Единственный результат n обязан быть 0; иначе публикация блокируется.
WITH d AS
    (SELECT e.*,
            try(json_parse(event_document)) AS j,
            try(json_parse(payload)) AS p
     FROM {{target:oil_dds__event}} e)
SELECT count(*) AS n
FROM d
WHERE j IS NULL
    OR p IS NULL
    OR event_id IS DISTINCT
    FROM json_extract_scalar(j, '$.event_id')
    OR event_type IS DISTINCT
    FROM json_extract_scalar(j, '$.event_type')
    OR entity_id IS DISTINCT
    FROM json_extract_scalar(j, '$.entity_id')
    OR entity_version IS DISTINCT
    FROM try_cast(json_extract_scalar(j, '$.entity_version') AS bigint)
    OR op IS DISTINCT
    FROM json_extract_scalar(j, '$.op')
    OR event_time IS DISTINCT
    FROM try(from_iso8601_timestamp_nanos(json_extract_scalar(j, '$.event_time')))
    OR json_format(p) IS DISTINCT
    FROM json_format(json_extract(j, '$.payload'))
    OR load_dts IS NULL
    OR record_source IS NULL
    OR batch_id IS NULL;
