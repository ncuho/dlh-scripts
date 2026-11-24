-- Проверяет обязательность/уникальность ключей и существование родителей; нарушение блокирует дочерние шаги.
SELECT count(*) n FROM {{target:oil_stg__event_log}} e LEFT JOIN {{target:oil_stg__batch_control}} b ON b.batch_id=e.batch_id WHERE e.batch_id IS NULL OR b.batch_id IS NULL;
