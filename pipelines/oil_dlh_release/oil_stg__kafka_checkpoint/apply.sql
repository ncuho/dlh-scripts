-- Обновляет существующий объект из предыдущего принятого snapshot и ограниченного окна raw; _next не создаётся.
-- Фиксирует следующий применённый offset целевой кандидатной истории; исходный checkpoint приёма GP не копируется как граница расчёта.
SELECT topic,kafka_partition,max(kafka_offset)+1 AS next_offset,max(received_at) AS updated_at
FROM {{target:oil_stg__event_log}} GROUP BY topic,kafka_partition;
