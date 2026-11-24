-- Копирует объект согласованного среза GP без пересчёта бизнес-логики; запись выполняет CedrusData Engine.
SELECT topic,kafka_partition,next_offset,updated_at FROM {{source:oil_stg__kafka_checkpoint}};
