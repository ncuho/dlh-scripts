-- Read-only проверки исходного контура; нулевое число означает, что gate пройден.
-- Не устанавливает и не предполагает DB-level enforcement PK/FK.
SELECT 'event_id_conflict' AS check_name, count(*) AS violations FROM (
 SELECT event_id FROM demo_ods.event GROUP BY event_id HAVING count(*)>1
) q
UNION ALL SELECT 'entity_version_conflict',count(*) FROM (
 SELECT event_type,entity_id,entity_version FROM demo_ods.event GROUP BY 1,2,3 HAVING count(*)>1
) q
UNION ALL SELECT 'hub_product_hash_collision',count(*) FROM (
 SELECT product_hk FROM demo_vault.hub_product GROUP BY product_hk HAVING count(DISTINCT product_id)>1
) q
UNION ALL SELECT 'hub_store_hash_collision',count(*) FROM (
 SELECT store_hk FROM demo_vault.hub_store GROUP BY store_hk HAVING count(DISTINCT store_id)>1
) q
UNION ALL SELECT 'shipment_fact_duplicate',count(*) FROM (
 SELECT shipment_hk FROM demo_dm.fact_shipment GROUP BY shipment_hk HAVING count(*)<>1
) q
UNION ALL SELECT 'return_fact_duplicate',count(*) FROM (
 SELECT return_hk FROM demo_dm.fact_return GROUP BY return_hk HAVING count(*)<>1
) q
UNION ALL SELECT 'orphan_return',count(*) FROM demo_dm.fact_return r
WHERE NOT EXISTS (SELECT 1 FROM demo_dm.fact_shipment s WHERE s.shipment_hk=r.shipment_hk)
UNION ALL SELECT 'amount_formula',count(*) FROM demo_dm.sales_day
WHERE net_amount<>shipment_amount-return_amount
UNION ALL SELECT 'currency_mismatch',count(*) FROM demo_dm.fact_return r
JOIN demo_dm.fact_shipment s ON s.shipment_hk=r.shipment_hk WHERE r.currency<>s.currency;
