-- исходный запрет неоднозначной даты начала версии месторождения.
SELECT count(*) AS n FROM (SELECT field_hk,event_time FROM {{target:oil_dds__sat_field}} GROUP BY field_hk,event_time HAVING count(*)>1) q;
