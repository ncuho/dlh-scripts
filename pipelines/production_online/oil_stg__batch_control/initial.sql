-- Учитывает состояния исходных партий и завершённость применения. История читается из C0; исходная GP-функция повторно не запускается.
SELECT batch_id,
       created_at,
       error_text,
       source_manifest,
       source_name,
       status
FROM {{source:oil_stg__batch_control}};
