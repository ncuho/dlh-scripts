-- Обновляет существующий объект из предыдущего принятого snapshot и ограниченного окна raw; _next не создаётся.
-- Сохраняет исходный аудит и создаёт родителей новых доставок: отрицательный ID однозначно задаётся partition/offset.
-- AS IS выдавал batch_id через sequence GP; TO BE сохраняет его для INITIAL и выделяет отдельный namespace для raw-хвоста.
WITH incoming AS (
 SELECT d.*, -(CAST(d.kafka_partition AS BIGINT)*1000000000000000+d.kafka_offset+1) AS target_batch_id
 FROM {{raw_window}} d WHERE NOT EXISTS (
  SELECT 1 FROM {{previous:oil_stg__event_log}} i WHERE i.topic=d.topic
   AND i.kafka_partition=d.kafka_partition AND i.kafka_offset=d.kafka_offset)
)
SELECT batch_id,created_at,error_text,source_manifest,source_name,status FROM {{previous:oil_stg__batch_control}}
UNION ALL
SELECT target_batch_id,min(received_at),CAST(NULL AS STRING),
 'Lakehouse delivery; original sink batch_id retained in bronze.__raw_events',
 'kafka:oil.events.v1','APPLIED' FROM incoming GROUP BY target_batch_id;
