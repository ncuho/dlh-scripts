-- Копирует объект согласованного среза GP без пересчёта бизнес-логики; запись выполняет CedrusData Engine.
SELECT batch_id,source_name,created_at,status,source_manifest,error_text FROM {{source:oil_stg__batch_control}};
