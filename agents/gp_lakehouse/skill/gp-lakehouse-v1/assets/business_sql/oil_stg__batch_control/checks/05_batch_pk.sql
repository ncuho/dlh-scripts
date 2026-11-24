-- Проверяет обязательность/уникальность ключей и существование родителей; нарушение блокирует дочерние шаги.
SELECT count(*) n FROM (SELECT batch_id FROM {{target:oil_stg__batch_control}} GROUP BY batch_id HAVING batch_id IS NULL OR count(*)<>1) q;
