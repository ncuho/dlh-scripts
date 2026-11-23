"""Trino SQL for immutable oil release. Requires prevalidated canonical_event.

Rendered queries were reconciled against GP on a fenced rehearsal boundary.
A new run still requires a live fence and the complete DQ gates.
"""

import re

SCHEMA = re.compile(r"^oil_r_[a-z0-9_]{1,42}$")


def plan(schema, chain="performance"):
    """Строит порядок миграции, зависимости и критерии проверки по фактическим входам."""
    if chain not in ("production", "performance"):
        raise ValueError("unknown business chain")
    if not SCHEMA.fullmatch(schema):
        raise ValueError("invalid release schema")
    t = "dlh." + schema
    hk = lambda x: "lower(to_hex(md5(to_utf8(" + x + "))))"
    field_hk = hk("'field:'||entity_id")
    well_hk = hk("'well:'||entity_id")
    parent_field = hk("'field:'||json_extract_scalar(document,'$.payload.field_id')")
    prod_hk = hk("'production:'||entity_id")
    parent_well = hk("'well:'||json_extract_scalar(document,'$.payload.well_id')")
    down_hk = hk("'downtime:'||entity_id")
    p = []
    # Release-scoped logical Raw Vault projection; ingestion metadata is not
    # reconstructed from raw sink fields and must not be compared as GP audit data.
    for entity, prefix, key in [
        ("field", "field", "field_hk"),
        ("well", "well", "well_hk"),
        ("production_day", "production", "production_hk"),
        ("downtime", "downtime", "downtime_hk"),
    ]:
        p.append(
            (
                "hub_" + entity,
                f"""
SELECT DISTINCT
    {hk(repr(prefix+':')+'||entity_id')} AS {key},
    entity_id
FROM
    {t}.canonical_event
WHERE
    event_type = '{entity}'
""",
            )
        )
    for (
        name,
        entity,
        left_prefix,
        left_key,
        right_prefix,
        right_key,
        right_field,
        link_prefix,
    ) in [
        (
            "link_well_field",
            "well",
            "well",
            "well_hk",
            "field",
            "field_hk",
            "field_id",
            "well_field",
        ),
        (
            "link_production_day_well",
            "production_day",
            "production",
            "production_hk",
            "well",
            "well_hk",
            "well_id",
            "production_day_well",
        ),
        (
            "link_downtime_well",
            "downtime",
            "downtime",
            "downtime_hk",
            "well",
            "well_hk",
            "well_id",
            "downtime_well",
        ),
    ]:
        parent = "json_extract_scalar(document,'$.payload." + right_field + "')"
        keyexpr = hk(repr(link_prefix + ":") + "||entity_id||':'||" + parent)
        p.append(
            (
                name,
                f"""
SELECT DISTINCT
    {keyexpr} AS link_hk,
    {hk(repr(left_prefix+':')+'||entity_id')} AS {left_key},
    {hk(repr(right_prefix+':')+'||'+parent)} AS {right_key}
FROM
    {t}.canonical_event
WHERE
    event_type = '{entity}'
""",
            )
        )
    for entity, prefix, key in [
        ("field", "field", "field_hk"),
        ("well", "well", "well_hk"),
        ("production_day", "production", "production_hk"),
        ("downtime", "downtime", "downtime_hk"),
    ]:
        p.append(
            (
                "sat_" + entity,
                f"""
SELECT
    {hk(repr(prefix+':')+'||entity_id')} AS {key},
    event_id,
    entity_version,
    op,
    event_time,
    document
FROM
    {t}.canonical_event
WHERE
    event_type = '{entity}'
""",
            )
        )
    p.append(
        (
            "dim_field",
            f"""
SELECT
    {field_hk} AS field_hk,
    entity_version,
    event_time AS valid_from,
    lead(event_time) OVER (PARTITION BY entity_id ORDER BY event_time, entity_version, event_id) AS valid_to,
    json_extract_scalar (document, '$.payload.field_name') AS field_name,
    json_extract_scalar (document, '$.payload.region') AS region,
    (op = 'D') AS is_deleted
FROM
    {t}.canonical_event
WHERE
    event_type = 'field'
""",
        )
    )
    p.append(
        (
            "dim_well",
            f"""
SELECT
    {well_hk} AS well_hk,
    {parent_field} AS field_hk,
    entity_version,
    CAST(json_extract_scalar (document, '$.payload.valid_from') AS DATE) AS valid_from,
    lead(CAST(json_extract_scalar (document, '$.payload.valid_from') AS DATE)) OVER (PARTITION BY entity_id ORDER BY CAST(json_extract_scalar (document, '$.payload.valid_from') AS date),
        entity_version,
        event_id) AS valid_to,
    json_extract_scalar (document, '$.payload.well_name') AS well_name,
    json_extract_scalar (document, '$.payload.status') AS status,
    (op = 'D') AS is_deleted
FROM
    {t}.canonical_event
WHERE
    event_type = 'well'
""",
        )
    )
    p.append(
        (
            "fact_well_day",
            f"""
WITH p AS (
    SELECT
        *,
        row_number() OVER (PARTITION BY entity_id ORDER BY entity_version DESC,
            event_id DESC) AS rn
    FROM
        {t}.canonical_event
    WHERE
        event_type = 'production_day'
),
decoded AS (
    SELECT
        {prod_hk} AS production_hk,
        {parent_well} AS well_hk,
        CAST(json_extract_scalar (document, '$.payload.production_date') AS date) AS production_date,
        CAST(json_extract_scalar (document, '$.payload.oil_tonnes') AS DECIMAL(18, 3)) AS oil_tonnes,
        json_extract_scalar (document, '$.payload.status') AS status
    FROM
        p
    WHERE
        rn = 1
        AND op = 'U'
)
SELECT
    p.production_hk,
    p.well_hk,
    w.field_hk,
    p.production_date,
    w.entity_version AS well_version,
    f.entity_version AS field_version,
    p.status,
    p.oil_tonnes
FROM
    decoded p
    JOIN {t}.dim_well w ON w.well_hk = p.well_hk
        AND NOT w.is_deleted
        AND w.valid_from <= p.production_date
        AND (w.valid_to IS NULL
            OR p.production_date < w.valid_to)
    JOIN {t}.dim_field f ON f.field_hk = w.field_hk
        AND NOT f.is_deleted
        AND f.valid_from <= (CAST(p.production_date AS timestamp) AT TIME ZONE 'UTC')
        AND (f.valid_to IS NULL
            OR (CAST(p.production_date AS timestamp) AT TIME ZONE 'UTC') < f.valid_to)
""",
        )
    )
    p.append(
        (
            "agg_field_day",
            f"""
SELECT
    p.production_date,
    p.field_hk,
    f.field_name,
    f.region,
    CAST(sum(p.oil_tonnes) AS DECIMAL(22, 3)) AS oil_tonnes,
    CAST(sum(
            CASE WHEN p.status = 'ACTIVE' THEN
                1
            ELSE
                0
            END) AS BIGINT) AS producing_well_days,
    CASE WHEN sum(
        CASE WHEN p.status = 'ACTIVE' THEN
            1
        ELSE
            0
        END) = 0 THEN
        CAST(NULL AS DECIMAL(22, 6))
    ELSE
        CAST(sum(p.oil_tonnes) / sum(
                CASE WHEN p.status = 'ACTIVE' THEN
                    1
                ELSE
                    0
                END) AS DECIMAL(22, 6))
    END AS oil_per_well_day
FROM
    {t}.fact_well_day p
    JOIN {t}.dim_field f ON f.field_hk = p.field_hk
        AND f.entity_version = p.field_version
GROUP BY
    p.production_date,
    p.field_hk,
    f.field_name,
    f.region
""",
        )
    )
    p.append(
        (
            "dim_downtime_reason",
            """
SELECT
    reason,
    description
FROM (
    VALUES ('EQUIPMENT', 'Оборудование'),
        ('POWER', 'Энергоснабжение'),
        ('PLANNED', 'Плановый простой')) AS v (reason, description)
""",
        )
    )
    p.append(
        (
            "fact_downtime_interval",
            f"""
WITH d AS (
    SELECT
        *,
        row_number() OVER (PARTITION BY entity_id ORDER BY entity_version DESC,
            event_id DESC) AS rn
    FROM
        {t}.canonical_event
    WHERE
        event_type = 'downtime'
),
decoded AS (
    SELECT
        {down_hk} AS downtime_hk,
        {parent_well} AS well_hk,
        from_iso8601_timestamp (json_extract_scalar (document, '$.payload.start_at')) AS start_at,
        from_iso8601_timestamp (json_extract_scalar (document, '$.payload.end_at')) AS end_at,
        json_extract_scalar (document, '$.payload.reason') AS reason
    FROM
        d
    WHERE
        rn = 1
        AND op = 'U'
)
SELECT
    d.downtime_hk,
    d.well_hk,
    p.field_hk,
    d.start_at,
    d.end_at,
    d.reason,
    CAST((to_unixtime (d.end_at) - to_unixtime (d.start_at)) / 3600 AS DECIMAL(16, 6)) AS hours
FROM
    decoded d
    JOIN {t}.fact_well_day p ON p.well_hk = d.well_hk
        AND p.production_date = CAST(at_timezone (d.start_at, 'UTC') AS DATE)
        AND p.status = 'ACTIVE'
""",
        )
    )
    p.append(
        (
            "agg_field_downtime_day",
            f"""
SELECT
    CAST(at_timezone (start_at, 'UTC') AS DATE) AS production_date,
    field_hk,
    CAST(sum(hours) AS DECIMAL(20, 6)) AS downtime_hours
FROM
    {t}.fact_downtime_interval
GROUP BY
    1,
    2
""",
        )
    )
    p.append(
        (
            "field_performance",
            f"""
SELECT
    p.*,
    coalesce(d.downtime_hours, DECIMAL '0.000000') AS downtime_hours
FROM
    {t}.agg_field_day p
    LEFT JOIN {t}.agg_field_downtime_day d ON d.production_date = p.production_date
        AND d.field_hk = p.field_hk
""",
        )
    )
    if chain == "production":
        excluded = {"hub_downtime", "link_downtime_well", "sat_downtime", "dim_downtime_reason", "fact_downtime_interval", "agg_field_downtime_day", "field_performance"}
        p = [(n, q) for n, q in p if n not in excluded]
    return [
        (
            name,
            "CREATE TABLE "
            + t
            + "."
            + name
            + """
WITH (
    format = 'PARQUET'
) AS
"""
            + sql,
        )
        for name, sql in p
    ]
