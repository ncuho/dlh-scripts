-- Полное сравнение INITIAL с исходным parsed STG, включая точный текст payload и исходные batch_id.
SELECT count(*) AS n
FROM (
          (SELECT batch_id,
                  topic,
                  kafka_partition,
                  kafka_offset,
                  event_json,
                  schema_version,
                  event_id,
                  event_type,
                  entity_id,
                  entity_version,
                  op,
                  event_time,
                  payload
           FROM {{source:oil_stg__v_event_parsed}}
           EXCEPT ALL SELECT batch_id,
                             topic,
                             kafka_partition,
                             kafka_offset,
                             event_json,
                             schema_version,
                             event_id,
                             event_type,
                             entity_id,
                             entity_version,
                             op,
                             event_time,
                             payload
           FROM {{target:oil_stg__v_event_parsed}})
      UNION ALL
          (SELECT batch_id,
                  topic,
                  kafka_partition,
                  kafka_offset,
                  event_json,
                  schema_version,
                  event_id,
                  event_type,
                  entity_id,
                  entity_version,
                  op,
                  event_time,
                  payload
           FROM {{target:oil_stg__v_event_parsed}}
           EXCEPT ALL SELECT batch_id,
                             topic,
                             kafka_partition,
                             kafka_offset,
                             event_json,
                             schema_version,
                             event_id,
                             event_type,
                             entity_id,
                             entity_version,
                             op,
                             event_time,
                             payload
           FROM {{source:oil_stg__v_event_parsed}})) q;


-- Мультимножество бизнес-полей соответствует текущему STG. Batch ID исключён; JSON payload здесь сравнивается по JSON-представлению, его точный текст дополнительно проверяется INITIAL и stage-to-ledger.
WITH stage AS
    (SELECT topic,
            kafka_partition,
            kafka_offset,
            event_json,
            try(json_parse(event_json)) AS j
     FROM {{target:oil_stg__event_log}}),
     expected AS
    (SELECT topic,
            kafka_partition,
            kafka_offset,
            event_json,
            try_cast(json_extract_scalar(j, '$.schema_version') AS integer) AS schema_version,
            json_extract_scalar(j, '$.event_id') AS event_id,
            json_extract_scalar(j, '$.event_type') AS event_type,
            json_extract_scalar(j, '$.entity_id') AS entity_id,
            try_cast(json_extract_scalar(j, '$.entity_version') AS bigint) AS entity_version,
            json_extract_scalar(j, '$.op') AS op,
            try(from_iso8601_timestamp_nanos(json_extract_scalar(j, '$.event_time'))) AS event_time,
            json_format(json_extract(j, '$.payload')) AS payload_json
     FROM stage),
     actual AS
    (SELECT topic,
            kafka_partition,
            kafka_offset,
            event_json,
            schema_version,
            event_id,
            event_type,
            entity_id,
            entity_version,
            op,
            event_time,
            json_format(try(json_parse(payload))) AS payload_json
     FROM {{target:oil_stg__v_event_parsed}})
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


-- Повторные одинаковые доставки допустимы; конфликт JSON на позиции или event_id, а также конфликт версии запрещены.
SELECT count(*) AS n
FROM
    (SELECT min(event_id) AS k
     FROM {{target:oil_stg__v_event_parsed}}
     GROUP BY topic,
              kafka_partition,
              kafka_offset
     HAVING topic IS NULL
     OR kafka_partition IS NULL
     OR kafka_partition<0
     OR kafka_offset IS NULL
     OR kafka_offset<0
     OR count(DISTINCT event_json)>1
     OR count_if(event_json IS NULL)>0
     UNION ALL SELECT event_id AS k
     FROM {{target:oil_stg__v_event_parsed}}
     GROUP BY event_id
     HAVING event_id IS NULL
     OR count(DISTINCT event_json)>1
     UNION ALL SELECT min(event_id) AS k
     FROM {{target:oil_stg__v_event_parsed}}
     GROUP BY event_type,
              entity_id,
              entity_version
     HAVING count(DISTINCT event_id)>1) q;


-- Исходные обязательные ограничения envelope и соответствие ID внутри payload; ошибки JSON и преобразований дают нарушения.
WITH p AS
    (SELECT e.*,
            try(json_parse(event_json)) AS j,
            try(json_parse(payload)) AS pj
     FROM {{target:oil_stg__v_event_parsed}} e)
SELECT count(*) AS n
FROM p
WHERE batch_id IS NULL
    OR j IS NULL
    OR pj IS NULL
    OR substr(json_format(j), 1, 1)<>'{'
    OR substr(json_format(pj), 1, 1)<>'{'
    OR schema_version IS DISTINCT
    FROM 1
    OR event_id IS NULL
    OR event_id=''
    OR event_type IS NULL
    OR event_type NOT IN ('field',
                          'well',
                          'production_day',
                          'downtime')
    OR entity_id IS NULL
    OR NOT regexp_like(entity_id, '^[A-Za-z0-9_-]+$')
    OR entity_version IS NULL
    OR entity_version<1
    OR op IS NULL
    OR op NOT IN ('U',
                  'D')
    OR event_time IS NULL
    OR event_time IS DISTINCT
    FROM try(from_iso8601_timestamp_nanos(json_extract_scalar(j, '$.event_time')))
    OR NOT coalesce(regexp_like(json_extract_scalar(j, '$.event_time'), '[Tt].*([Zz]|[+-][0-9]{2}:[0-9]{2})$'), FALSE)
    OR json_extract_scalar(pj, CASE event_type
                                   WHEN 'production_day' THEN '$.production_id'
                                   WHEN 'field' THEN '$.field_id'
                                   WHEN 'well' THEN '$.well_id'
                                   WHEN 'downtime' THEN '$.downtime_id'
                               END) IS DISTINCT
    FROM entity_id;


-- Обязательные атрибуты field/well и поддерживаемая ISO-дата valid_from; D проверяется так же, как U.
WITH p AS
    (SELECT event_type,
            try(json_parse(payload)) AS j
     FROM {{target:oil_stg__v_event_parsed}})
SELECT count(*) AS n
FROM p
WHERE (event_type='field'
       AND (nullif(json_extract_scalar(j, '$.field_name'), '') IS NULL
            OR nullif(json_extract_scalar(j, '$.region'), '') IS NULL))
    OR (event_type='well'
        AND (json_extract_scalar(j, '$.field_id') IS NULL
             OR NOT regexp_like(json_extract_scalar(j, '$.field_id'), '^[A-Za-z0-9_-]+$')
             OR nullif(json_extract_scalar(j, '$.well_name'), '') IS NULL
             OR json_extract_scalar(j, '$.valid_from') IS NULL
             OR NOT regexp_like(json_extract_scalar(j, '$.valid_from'), '^[0-9]{4}-[0-9]{2}-[0-9]{2}$')
             OR try_cast(json_extract_scalar(j, '$.valid_from') AS date) IS NULL
             OR json_extract_scalar(j, '$.status') IS NULL
             OR json_extract_scalar(j, '$.status') NOT IN ('ACTIVE',
                                                           'INACTIVE')));


-- Ограничения production_day, составной ID и decimal(18,3). Отрицательное исходное число отвергается даже при округлении к нулю.
WITH p AS
    (SELECT entity_id,
            try(json_parse(payload)) AS j
     FROM {{target:oil_stg__v_event_parsed}}
     WHERE event_type='production_day'),
     v AS
    (SELECT entity_id,
            json_extract_scalar(j, '$.well_id') AS well_id,
            json_extract_scalar(j, '$.production_date') AS production_date,
            json_extract_scalar(j, '$.oil_tonnes') AS oil_tonnes,
            json_extract_scalar(j, '$.status') AS status
     FROM p)
SELECT count(*) AS n
FROM v
WHERE well_id IS NULL
    OR NOT regexp_like(well_id, '^[A-Za-z0-9_-]+$')
    OR production_date IS NULL
    OR NOT regexp_like(production_date, '^[0-9]{4}-[0-9]{2}-[0-9]{2}$')
    OR try_cast(production_date AS date) IS NULL
    OR oil_tonnes IS NULL
    OR NOT regexp_like(oil_tonnes, '^[+-]?([0-9]+([.][0-9]*)?|[.][0-9]+)([eE][+-]?[0-9]+)?$')
    OR try_cast(oil_tonnes AS decimal(18, 3)) IS NULL
    OR (substr(oil_tonnes, 1, 1)='-'
        AND regexp_like(regexp_extract(oil_tonnes, '^[+-]?([^eE]+)', 1), '[1-9]'))
    OR status IS NULL
    OR status NOT IN ('ACTIVE',
                      'INACTIVE')
    OR entity_id IS DISTINCT
    FROM (well_id || '_' || replace(production_date, '-', ''));


-- Downtime остаётся в ledger и проходит исходные ограничения интервала. Дата конца учитывает исключённую последнюю микросекунду без потери точности на полуночи.
WITH p AS
    (SELECT try(json_parse(payload)) AS j
     FROM {{target:oil_stg__v_event_parsed}}
     WHERE event_type='downtime'),
     v AS
    (SELECT json_extract_scalar(j, '$.well_id') AS well_id,
            json_extract_scalar(j, '$.reason') AS reason,
            json_extract_scalar(j, '$.start_at') AS start_text,
            json_extract_scalar(j, '$.end_at') AS end_text,
            try(from_iso8601_timestamp_nanos(json_extract_scalar(j, '$.start_at'))) AS start_at,
            try(from_iso8601_timestamp_nanos(json_extract_scalar(j, '$.end_at'))) AS end_at
     FROM p),
     u AS
    (SELECT v.*,
            at_timezone(start_at, 'UTC') AS start_utc,
            at_timezone(end_at, 'UTC') AS end_utc
     FROM v)
SELECT count(*) AS n
FROM u
WHERE well_id IS NULL
    OR NOT regexp_like(well_id, '^[A-Za-z0-9_-]+$')
    OR reason IS NULL
    OR reason NOT IN ('EQUIPMENT',
                      'POWER',
                      'PLANNED')
    OR start_at IS NULL
    OR end_at IS NULL
    OR NOT coalesce(regexp_like(start_text, '[Tt].*([Zz]|[+-][0-9]{2}:[0-9]{2})$'), FALSE)
    OR NOT coalesce(regexp_like(end_text, '[Tt].*([Zz]|[+-][0-9]{2}:[0-9]{2})$'), FALSE)
    OR start_at IS DISTINCT
    FROM try_cast(start_at AS timestamp(6) WITH TIME ZONE)
    OR end_at IS DISTINCT
    FROM try_cast(end_at AS timestamp(6) WITH TIME ZONE)
    OR end_at<=start_at
    OR cast(start_utc AS date) IS DISTINCT
    FROM (CASE
              WHEN end_utc=date_trunc('day', end_utc) THEN date_add('day', -1, cast(end_utc AS date))
              ELSE cast(end_utc AS date)
          END);
