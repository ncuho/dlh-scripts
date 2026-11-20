-- PLAN ONLY: идентификатор source GP в CedrusData и фактические каталоги
-- проверить перед выполнением. Сравниваются денежные DECIMAL, не DOUBLE.
-- Один и тот же release/batch и одна согласованная граница — обязательны.
WITH src AS (
 SELECT business_date,region,category,currency,shipment_amount,return_amount,net_amount,shipped_qty,returned_qty
 FROM greenplum_gp.demo_dm.sales_day
), tgt AS (
 SELECT business_date,region,category,currency,shipment_amount,return_amount,net_amount,shipped_qty,returned_qty
 FROM dlh.commerce_dm.sales_day
)
SELECT coalesce(s.business_date,t.business_date) AS business_date,
       coalesce(s.region,t.region) AS region,coalesce(s.category,t.category) AS category,
       coalesce(s.currency,t.currency) AS currency,
       s.shipment_amount AS gp_shipment,t.shipment_amount AS dlh_shipment,
       s.return_amount AS gp_returns,t.return_amount AS dlh_returns,
       s.net_amount AS gp_net,t.net_amount AS dlh_net
FROM src s FULL OUTER JOIN tgt t
 ON s.business_date=t.business_date AND s.region=t.region AND s.category=t.category AND s.currency=t.currency
WHERE s.business_date IS NULL OR t.business_date IS NULL
   OR s.shipment_amount IS DISTINCT FROM t.shipment_amount
   OR s.return_amount IS DISTINCT FROM t.return_amount
   OR s.net_amount IS DISTINCT FROM t.net_amount
   OR s.shipped_qty IS DISTINCT FROM t.shipped_qty
   OR s.returned_qty IS DISTINCT FROM t.returned_qty;
