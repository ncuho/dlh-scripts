-- TEMPLATE ONLY: do not run without resolving and validating each identifier.
-- Prerequisite: frozen GP source at the same verified cut as target release.
-- Replace <GP_CATALOG>, <RELEASE_ID>; the released Iceberg snapshot IDs in the
-- manifest must still be current or query each pinned snapshot with supported syntax.
-- Every result must be empty. EXCEPT ALL compares multiplicities (duplicates matter).
-- Confirm types/precision/timezone equivalence and Trino/connector support first.
WITH src AS (
 SELECT business_date,region,category,currency,shipment_amount,return_amount,
        net_amount,shipped_qty,returned_qty FROM <GP_CATALOG>.demo_dm.sales_day
), dst AS (
 SELECT business_date,region,category,currency,shipment_amount,return_amount,
        net_amount,shipped_qty,returned_qty FROM dlh.commerce_dm.r_<RELEASE_ID>_sales_day
), only_src AS (SELECT * FROM src EXCEPT ALL SELECT * FROM dst),
   only_dst AS (SELECT * FROM dst EXCEPT ALL SELECT * FROM src)
SELECT 'only_gp' AS mismatch, * FROM only_src
UNION ALL SELECT 'only_lakehouse' AS mismatch, * FROM only_dst;


-- dim_product: a zero-row result is required.
WITH src AS (SELECT product_hk,entity_version,valid_from,valid_to,product_name,category,is_deleted FROM <GP_CATALOG>.demo_dm.dim_product),
     dst AS (SELECT product_hk,entity_version,valid_from,valid_to,product_name,category,is_deleted FROM dlh.commerce_dm.r_<RELEASE_ID>_dim_product),
     only_src AS (SELECT * FROM src EXCEPT ALL SELECT * FROM dst),
     only_dst AS (SELECT * FROM dst EXCEPT ALL SELECT * FROM src)
SELECT 'only_gp' AS mismatch, * FROM only_src
UNION ALL SELECT 'only_lakehouse' AS mismatch, * FROM only_dst;

-- dim_store: a zero-row result is required.
WITH src AS (SELECT store_hk,entity_version,valid_from,valid_to,store_name,region,is_deleted FROM <GP_CATALOG>.demo_dm.dim_store),
     dst AS (SELECT store_hk,entity_version,valid_from,valid_to,store_name,region,is_deleted FROM dlh.commerce_dm.r_<RELEASE_ID>_dim_store),
     only_src AS (SELECT * FROM src EXCEPT ALL SELECT * FROM dst),
     only_dst AS (SELECT * FROM dst EXCEPT ALL SELECT * FROM src)
SELECT 'only_gp' AS mismatch, * FROM only_src
UNION ALL SELECT 'only_lakehouse' AS mismatch, * FROM only_dst;

-- fact_shipment: a zero-row result is required.
WITH src AS (SELECT shipment_hk,shipment_id,product_hk,store_hk,product_version,store_version,business_date,quantity,unit_price,currency,amount FROM <GP_CATALOG>.demo_dm.fact_shipment),
     dst AS (SELECT shipment_hk,shipment_id,product_hk,store_hk,product_version,store_version,business_date,quantity,unit_price,currency,amount FROM dlh.commerce_dm.r_<RELEASE_ID>_fact_shipment),
     only_src AS (SELECT * FROM src EXCEPT ALL SELECT * FROM dst),
     only_dst AS (SELECT * FROM dst EXCEPT ALL SELECT * FROM src)
SELECT 'only_gp' AS mismatch, * FROM only_src
UNION ALL SELECT 'only_lakehouse' AS mismatch, * FROM only_dst;

-- fact_return: a zero-row result is required.
WITH src AS (SELECT return_hk,return_id,shipment_hk,product_hk,store_hk,product_version,store_version,business_date,quantity,unit_price,currency,amount FROM <GP_CATALOG>.demo_dm.fact_return),
     dst AS (SELECT return_hk,return_id,shipment_hk,product_hk,store_hk,product_version,store_version,business_date,quantity,unit_price,currency,amount FROM dlh.commerce_dm.r_<RELEASE_ID>_fact_return),
     only_src AS (SELECT * FROM src EXCEPT ALL SELECT * FROM dst),
     only_dst AS (SELECT * FROM dst EXCEPT ALL SELECT * FROM src)
SELECT 'only_gp' AS mismatch, * FROM only_src
UNION ALL SELECT 'only_lakehouse' AS mismatch, * FROM only_dst;

-- Counts must also be reported and differences triaged; full multiset scans can be expensive.
