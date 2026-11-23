-- Из well payload получаем field ID и строим детерминированную связь ключей скважины/месторождения. Это целевой SQL-этап вместо части apply_batch GP.
SELECT link_hk,
       well_hk,
       field_hk,
       load_dts,
       record_source
FROM {{source:oil_dds__link_well_field}};
