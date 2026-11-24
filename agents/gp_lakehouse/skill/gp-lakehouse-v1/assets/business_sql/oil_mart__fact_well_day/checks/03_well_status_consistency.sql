-- исходная проверка согласованности статуса и нулевой добычи INACTIVE.
SELECT count(*) AS n FROM {{target:oil_mart__fact_well_day}} p JOIN {{target:oil_mart__dim_well}} w ON p.well_hk=w.well_hk AND p.well_version=w.entity_version WHERE p.status<>w.status OR (p.status='INACTIVE' AND p.oil_tonnes<>0);
