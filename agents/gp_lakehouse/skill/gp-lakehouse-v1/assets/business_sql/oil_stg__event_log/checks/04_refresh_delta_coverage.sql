-- каждая доставка delta представлена в результате той же позицией и тем же документом.
SELECT count(*) AS n FROM {{raw_window}} d WHERE NOT EXISTS (SELECT 1 FROM {{target:oil_stg__event_log}} t WHERE t.topic=d.topic AND t.kafka_partition=d.kafka_partition AND t.kafka_offset=d.kafka_offset AND t.event_json=d.event_json);
