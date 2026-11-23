# Gates(schema, chain) проверяет контракт событий, версии, SCD2 и покрытие фактов. comparisons(schema, chain) сравнивает полную историю и бизнес-таблицы через двусторонний EXCEPT ALL на одной удерживаемой границе. Каждый результат должен быть.
# Успешный INSERT или count без нарушений не заменяет полный набор проверок.
"""Read-only DQ gates and two-way Greenplum comparisons for oil releases.

A zero result for every query is necessary, not proof of a frozen source cut.
"""

from sql_plan import SCHEMA


def gates(schema, chain="performance"):
    """Формирует блокирующие SQL проверки уникальности, ссылок и бизнес-инвариантов."""
    if not SCHEMA.fullmatch(schema):
        raise ValueError("invalid release schema")
    t = "dlh." + schema
    checks = [
        (
            "event_id_conflict",
            f"SELECT count(*) AS n FROM (SELECT json_extract_scalar(document,'$.event_id') AS event_id FROM {t}.canonical_input GROUP BY 1 HAVING count(DISTINCT document)>1)",
        ),
        (
            "entity_version_conflict",
            f"SELECT count(*) AS n FROM (SELECT event_type,entity_id,entity_version FROM {t}.canonical_event GROUP BY 1,2,3 HAVING count(DISTINCT event_id)>1)",
        ),
        (
            "invalid_envelope",
            f"""SELECT count(*) AS n FROM {t}.canonical_event WHERE event_id IS NULL OR event_id='' OR event_type IS NULL OR event_type NOT IN ('field','well','production_day','downtime') OR entity_id IS NULL OR NOT regexp_like(entity_id,'^[A-Za-z0-9_-]+$') OR entity_version IS NULL OR entity_version<1 OR op IS NULL OR op NOT IN ('U','D') OR event_time IS NULL OR try(json_extract_scalar(document,'$.schema_version')) IS DISTINCT FROM '1' OR try(json_extract(document,'$.payload')) IS NULL""",
        ),
        (
            "ambiguous_field_time",
            f"SELECT count(*) AS n FROM (SELECT field_hk,valid_from FROM {t}.dim_field GROUP BY 1,2 HAVING count(*)>1)",
        ),
        (
            "ambiguous_well_time",
            f"SELECT count(*) AS n FROM (SELECT well_hk,valid_from FROM {t}.dim_well GROUP BY 1,2 HAVING count(*)>1)",
        ),
        (
            "production_fact_coverage",
            f"""WITH latest AS (SELECT *,row_number() OVER(PARTITION BY entity_id ORDER BY entity_version DESC,event_id DESC) rn FROM {t}.canonical_event WHERE event_type='production_day')
 SELECT count(*) AS n FROM latest p LEFT JOIN (SELECT production_hk,count(*) n FROM {t}.fact_well_day GROUP BY 1) f ON f.production_hk=lower(to_hex(md5(to_utf8('production:'||p.entity_id)))) WHERE p.rn=1 AND p.op='U' AND coalesce(f.n,0)<>1""",
        ),
        (
            "downtime_fact_coverage",
            f"""WITH latest AS (SELECT *,row_number() OVER(PARTITION BY entity_id ORDER BY entity_version DESC,event_id DESC) rn FROM {t}.canonical_event WHERE event_type='downtime')
 SELECT count(*) AS n FROM latest p LEFT JOIN (SELECT downtime_hk,count(*) n FROM {t}.fact_downtime_interval GROUP BY 1) f ON f.downtime_hk=lower(to_hex(md5(to_utf8('downtime:'||p.entity_id)))) WHERE p.rn=1 AND p.op='U' AND coalesce(f.n,0)<>1""",
        ),
        (
            "bad_production",
            f"SELECT count(*) AS n FROM {t}.fact_well_day WHERE status NOT IN ('ACTIVE','INACTIVE') OR status IS NULL OR oil_tonnes IS NULL OR oil_tonnes<0 OR (status='INACTIVE' AND oil_tonnes<>0)",
        ),
        (
            "duplicate_well_day",
            f"SELECT count(*) AS n FROM (SELECT well_hk,production_date FROM {t}.fact_well_day GROUP BY 1,2 HAVING count(*)>1)",
        ),
        (
            "well_status_mismatch",
            f"SELECT count(*) AS n FROM {t}.fact_well_day p JOIN {t}.dim_well w ON w.well_hk=p.well_hk AND w.entity_version=p.well_version WHERE p.status IS DISTINCT FROM w.status",
        ),
        (
            "bad_downtime",
            f"SELECT count(*) AS n FROM {t}.fact_downtime_interval WHERE end_at<=start_at OR reason NOT IN ('EQUIPMENT','POWER','PLANNED') OR reason IS NULL OR CAST(at_timezone(start_at,'UTC') AS DATE)<>CAST(at_timezone(end_at-INTERVAL '0.001' SECOND,'UTC') AS DATE)",
        ),
        (
            "overlap",
            f"SELECT count(*) AS n FROM (SELECT well_hk,start_at,end_at,max(end_at) OVER(PARTITION BY well_hk ORDER BY start_at,end_at ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) previous_end FROM {t}.fact_downtime_interval) WHERE previous_end>start_at",
        ),
    ]

    if chain == "production":
        checks = [(n, q.replace(",'downtime'", "")) for n, q in checks if n not in {"downtime_fact_coverage", "bad_downtime", "overlap"}]
    return checks


def comparisons(schema, chain="performance"):
    """Формирует двунаправленные SQL сравнения источника и цели на одной границе."""
    if not SCHEMA.fullmatch(schema):
        raise ValueError("invalid release schema")
    # Compare complete event history, not just a few business aggregates. The
    # Target has exactly one copy of each byte-identical replay by construction.
    source_events = "SELECT event_document AS document FROM greenplum_gp.oil_dds.event WHERE event_type IN ('field','well','production_day','downtime')"
    if chain == "production":
        source_events = source_events.replace(",'downtime'", "")
    target_events = "SELECT document FROM dlh." + schema + ".canonical_event"
    result = [
        (
            "complete_event_history",
            "SELECT count(*) AS n FROM (("
            + source_events
            + " EXCEPT ALL "
            + target_events
            + ") UNION ALL ("
            + target_events
            + " EXCEPT ALL "
            + source_events
            + "))",
        )
    ]
    targets = [
        "dim_field",
        "dim_well",
        "fact_well_day",
        "agg_field_day",
        "dim_downtime_reason",
        "fact_downtime_interval",
        "agg_field_downtime_day",
        "field_performance",
    ]
    if chain == "production":
        targets = targets[:4]
    for name in targets:
        source = "v_field_performance" if name == "field_performance" else name
        a = "SELECT * FROM greenplum_gp.oil_mart." + source
        b = "SELECT * FROM dlh." + schema + "." + name
        result.append(
            (
                name,
                "SELECT count(*) AS n FROM (("
                + a
                + " EXCEPT ALL "
                + b
                + ") UNION ALL ("
                + b
                + " EXCEPT ALL "
                + a
                + "))",
            )
        )
    return result
