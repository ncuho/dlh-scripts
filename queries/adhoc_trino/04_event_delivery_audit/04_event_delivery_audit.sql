/* Считает доставки и уникальные позиции Kafka, а также показывает границы событий по дням приёма. */
SELECT
  CAST((
    AT_TIMEZONE(received_at, 'UTC')
  ) AS DATE) AS received_date,
  kafka_partition,
  COUNT(*) AS deliveries,
  COUNT(DISTINCT kafka_offset) AS unique_offsets,
  MIN(kafka_offset) AS first_offset,
  MAX(kafka_offset) AS last_offset,
  COUNT(*) FILTER(WHERE
    JSON_EXTRACT_SCALAR(JSON_PARSE(event_json), '$.event_type') = 'production_day') AS production_events
FROM dlh.bronze.event_log
GROUP BY
  CAST((
    AT_TIMEZONE(received_at, 'UTC')
  ) AS DATE),
  kafka_partition
ORDER BY
  received_date DESC NULLS FIRST,
  kafka_partition;
