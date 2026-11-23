-- исходный запрет неоднозначной даты начала версии скважины.
SELECT count(*) AS n FROM (SELECT well_hk,valid_from FROM {{target:oil_dds__sat_well}} GROUP BY well_hk,valid_from HAVING count(*)>1) q;
