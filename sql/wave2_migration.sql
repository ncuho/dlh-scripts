-- PROJECT ONLY. Do not run until a new consistent GP/Kafka cut is frozen,
-- the first-wave shared dependencies are reconciled at that same cut, the
-- source Kafka consumer is fenced and a single target writer is enforced.
-- These statements are separate Iceberg commits, not one transaction. U/D
-- below selects the latest complete GP event state; this is a snapshot load,
-- not a tested continuous CDC or an incremental merge of first-wave changes.
-- Verify access/target nonexistence; compare input duplicate event_id payloads
-- before INSERT. Retain the source snapshot and offsets for recovery.

INSERT INTO dlh.oil_stg.event_log
SELECT e.* FROM greenplum_gp.oil_stg.event_log e
JOIN greenplum_gp.oil_stg.v_event_parsed p
  ON e.topic=p.topic AND e.kafka_partition=p.kafka_partition AND e.kafka_offset=p.kafka_offset
WHERE p.event_type='downtime'
  AND NOT EXISTS (SELECT 1 FROM dlh.oil_stg.event_log d
                  WHERE d.topic=e.topic AND d.kafka_partition=e.kafka_partition AND d.kafka_offset=e.kafka_offset);

INSERT INTO dlh.oil_dds.event
SELECT e.* FROM greenplum_gp.oil_dds.event e
WHERE e.event_type='downtime'
  AND NOT EXISTS (SELECT 1 FROM dlh.oil_dds.event d WHERE d.event_id=e.event_id);

CREATE TABLE dlh.oil_dds.hub_downtime WITH (format='PARQUET') AS SELECT * FROM greenplum_gp.oil_dds.hub_downtime;
CREATE TABLE dlh.oil_dds.link_downtime_well WITH (format='PARQUET') AS SELECT * FROM greenplum_gp.oil_dds.link_downtime_well;
CREATE TABLE dlh.oil_dds.sat_downtime WITH (format='PARQUET') AS SELECT * FROM greenplum_gp.oil_dds.sat_downtime;

CREATE TABLE dlh.oil_mart.dim_downtime_reason WITH (format='PARQUET') AS
SELECT reason, description FROM (VALUES
 ('EQUIPMENT','Оборудование'),('POWER','Энергоснабжение'),('PLANNED','Плановый простой')
) AS t(reason,description);

-- Matching synthetic baseline uses UTC timestamps at whole-second precision.
-- Verify decimal rounding and compare each row before accepting the release.
CREATE TABLE dlh.oil_mart.fact_downtime_interval WITH (format='PARQUET') AS
SELECT d.downtime_hk,l.well_hk,p.field_hk,d.start_at,d.end_at,d.reason,
 CAST((to_unixtime(d.end_at)-to_unixtime(d.start_at))/3600 AS DECIMAL(16,6)) AS hours
FROM (SELECT *,row_number() OVER(PARTITION BY downtime_hk ORDER BY entity_version DESC,event_id DESC) rn
      FROM dlh.oil_dds.sat_downtime) d
JOIN dlh.oil_dds.link_downtime_well l ON l.link_hk=d.link_hk
JOIN dlh.oil_mart.fact_well_day p ON p.well_hk=l.well_hk
  AND p.production_date=CAST(at_timezone(d.start_at,'UTC') AS DATE) AND p.status='ACTIVE'
WHERE d.rn=1 AND d.op='U';

CREATE TABLE dlh.oil_mart.agg_field_downtime_day WITH (format='PARQUET') AS
SELECT CAST(at_timezone(start_at,'UTC') AS DATE) AS production_date,field_hk,
       CAST(sum(hours) AS DECIMAL(20,6)) AS downtime_hours
FROM dlh.oil_mart.fact_downtime_interval GROUP BY 1,2;

-- A physical serving table is used here because this stand's LakeKeeper/OPA
-- currently denies SELECT from the analogous target view. Reconcile GP view
-- against this table on the same cut; configure atomic release externally.
CREATE TABLE dlh.oil_mart.field_performance WITH (format='PARQUET') AS
SELECT p.*,coalesce(d.downtime_hours,DECIMAL '0.000000') AS downtime_hours
FROM dlh.oil_mart.agg_field_day p LEFT JOIN dlh.oil_mart.agg_field_downtime_day d
 ON d.production_date=p.production_date AND d.field_hk=p.field_hk;
