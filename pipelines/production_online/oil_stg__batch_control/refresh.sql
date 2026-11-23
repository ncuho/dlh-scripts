-- Учитывает состояния исходных партий и завершённость применения. Здесь сохраняется аудит GP на C0, а не checkpoint нового sink.
SELECT batch_id,
       created_at,
       error_text,
       source_manifest,
       source_name,
       status
FROM {{initial:oil_stg__batch_control}};
