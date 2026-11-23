-- Хранит историческую связь скважино-дня со скважиной по техническим ключам. Изменяемые атрибуты находятся в satellites.
-- Engine: trino. Столбцы INSERT берутся в порядке DDL, не по позиции исходного SELECT.
-- INITIAL: согласованный исходный образ C0; исторический аудит не пересоздаётся.
SELECT link_hk,
       production_hk,
       well_hk,
       load_dts,
       record_source
FROM {{source:oil_dds__link_production_day_well}};
