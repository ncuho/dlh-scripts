"""Register a validated, materialized oil release and update BI lineage after switch.

No kubectl, credentials, consumer activation, dataset changes or destructive table
operations. A failure leaves a partial, idempotently resumable catalog registration.
"""

import argparse, ast, hashlib, json, os, re, ssl, sys, urllib.request
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent / "target"))
from cedrus_release_runner import MCP, snap
from bounded_delta import manifest as read_manifest
import sql_plan

DB = "dlh_trino.dlh"
OPERATOR = "d07fc8f6-2e54-4737-b503-890d73e43c89"
TEMPLATE = "dlh_trino.dlh.oil_demo_template.field_performance"
CHARTS = tuple("webinar_superset.chart_" + str(n) for n in range(19, 23))
NAME = re.compile(r"^[a-z][a-z0-9_]{0,62}$")


def decoded(result):
    content = result.get("content") or []
    if not content:
        raise RuntimeError("empty catalog result")
    v = content[0].get("text") if isinstance(content[0], dict) else content[0]
    if isinstance(v, str):
        try:
            v = json.loads(v)
        except json.JSONDecodeError:
            v = ast.literal_eval(v)
    return v


def tool(c, name, args):
    return decoded(c.rpc("tools/call", {"name": name, "arguments": args}))


def inventory(c, kind):
    name = "list_" + kind
    entries = []
    offset = 0
    while True:
        page = tool(c, name, {"database": DB, "limit": 100, "offset": offset})
        if not isinstance(page, dict) or not isinstance(page.get("data"), list):
            raise RuntimeError("invalid catalog listing")
        rows = page["data"]
        entries.extend(rows)
        total = page.get("paging", {}).get("total")
        if type(total) is not int:
            raise RuntimeError("catalog total missing")
        if len(entries) >= total:
            return {e["fullyQualifiedName"]: e for e in entries}
        if not rows:
            raise RuntimeError("catalog pagination incomplete")
        offset += len(rows)
        if offset > 100000:
            raise RuntimeError("catalog pagination overflow")


def coldef(row):
    n = row.get("Column")
    raw = str(row.get("Type", "")).lower()
    if not isinstance(n, str) or not NAME.fullmatch(n):
        raise ValueError("unsupported column identifier")
    base = raw.split("(")[0].split(" ")[0]
    types = {
        "varchar": "TEXT",
        "char": "TEXT",
        "bigint": "BIGINT",
        "integer": "INT",
        "smallint": "SMALLINT",
        "boolean": "BOOLEAN",
        "date": "DATE",
        "double": "DOUBLE",
        "real": "FLOAT",
        "decimal": "DECIMAL",
        "timestamp": "TIMESTAMPZ" if "with time zone" in raw else "TIMESTAMP",
    }
    if base not in types:
        raise ValueError("unsupported catalog type " + base)
    o = {
        "name": n,
        "dataType": types[base],
        "dataTypeDisplay": raw,
        "description": "Поле подтверждённого релиза на зафиксированной границе; синтетический нефтегазовый сценарий.",
    }
    if base == "decimal":
        m = re.fullmatch(r"decimal\((\d+),\s*(\d+)\)", raw)
        if not m:
            raise ValueError("bad decimal")
        o.update(precision=int(m.group(1)), scale=int(m.group(2)))
    return o


def upsert_edge(c, a, b, description, mappings=None):
    d = {"source": "Manual", "description": description}
    if mappings:
        d["columnsLineage"] = mappings
    tool(
        c,
        "add_lineage",
        {
            "lineage_data": {
                "edge": {
                    "fromEntity": {"id": a["id"], "type": "table"},
                    "toEntity": {"id": b["id"], "type": "table"},
                    "lineageDetails": d,
                }
            }
        },
    )


def superseted_binding(schema, dataset):
    url = os.environ["OIL_SUPERSET_URL"].rstrip("/")
    if not url.startswith("https://"):
        raise ValueError("Superset URL must be HTTPS")
    token = Path(os.environ["OIL_SUPERSET_BEARER_FILE"]).read_text().strip()
    ctx = ssl.create_default_context(cafile=os.environ.get("OIL_SUPERSET_CA_FILE"))
    req = urllib.request.Request(
        url + "/api/v1/dataset/" + str(dataset),
        headers={"Authorization": "Bearer " + token, "Accept": "application/json"},
    )
    with urllib.request.urlopen(req, context=ctx, timeout=30) as response:
        v = json.load(response)["result"]
    if (
        v.get("schema") != schema
        or v.get("table_name") != "field_performance"
        or v.get("database", {}).get("id") != 2
    ):
        raise ValueError("dataset switch not confirmed")
    for n in range(19, 23):
        q = urllib.request.Request(
            url + "/api/v1/chart/" + str(n),
            headers={"Authorization": "Bearer " + token, "Accept": "application/json"},
        )
        with urllib.request.urlopen(q, context=ctx, timeout=30) as response:
            chart = json.load(response)["result"]
        # Superset 6 returns datasource_id and datasource_type for saved charts.
        if (
            chart.get("datasource_id") != dataset
            or chart.get("datasource_type") != "table"
        ):
            raise ValueError("chart binding changed")


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--evidence", required=True)
    p.add_argument("--manifest", required=True)
    p.add_argument("--phase", choices=("ready", "published"), required=True)
    p.add_argument("--dataset-id", type=int)
    p.add_argument("--publication")
    a = p.parse_args()
    e = json.loads(Path(a.evidence).read_text())
    raw = Path(a.manifest).read_bytes()
    x = read_manifest(a.manifest)
    schema = e.get("schema", "")
    if (
        not sql_plan.SCHEMA.fullmatch(schema)
        or e.get("status") != "ready"
        or e.get("manifest_sha256") != hashlib.sha256(raw).hexdigest()
    ):
        raise ValueError("release evidence not ready or manifest mismatch")
    if e.get("cut_id") != x["cut_id"] or e.get("input_snapshots") != {
        "baseline": x["baseline_snapshot"],
        "delta": x["delta_snapshot"],
    }:
        raise ValueError("release cut mismatch")
    expected = ["canonical_input", "canonical_event"] + [
        n for n, _ in sql_plan.plan(schema)
    ]
    if (
        set(e.get("snapshots", {})) != set(expected)
        or not e.get("checks")
        or any(v != 0 for v in e["checks"].values())
    ):
        raise ValueError("release snapshots or DQ invalid")
    if a.phase == "published":
        if a.dataset_id != 8 or not a.publication:
            raise ValueError("dataset 8 and publication evidence required")
        publication = json.loads(Path(a.publication).read_text())
        if (
            publication.get("status") != "switched"
            or publication.get("dataset_id") != 8
            or publication.get("new")
            != {"schema": schema, "table_name": "field_performance"}
        ):
            raise ValueError("publication evidence mismatch")
        superseted_binding(schema, a.dataset_id)
    t = MCP(
        os.environ["OIL_TRINO_MCP_URL"],
        os.environ.get("OIL_MCP_CA_FILE"),
        os.environ.get("OIL_MCP_BEARER_FILE"),
    )
    for key, sid in e["snapshots"].items():
        if type(sid) is not int or snap(t, "dlh." + schema + "." + key) != sid:
            raise ValueError("release snapshot changed")
    for side in ("baseline", "delta"):
        if snap(t, x[side + "_table"]) != x[side + "_snapshot"]:
            raise ValueError("input snapshot changed")
    c = MCP(
        os.environ["OIL_CATALOG_MCP_URL"],
        os.environ.get("OIL_CATALOG_CA_FILE"),
        os.environ.get("OIL_CATALOG_BEARER_FILE"),
    )
    schemas = inventory(c, "schemas")
    tables = inventory(c, "tables")

    def register_schema(n):
        fq = DB + "." + n
        if fq not in schemas:
            z = tool(
                c,
                "create_schema",
                {
                    "schema_data": {
                        "name": n,
                        "database": DB,
                        "description": "Фактический изолированный release synthetic oil; не автоматический production cutover.",
                    }
                },
            )
            if z.get("fullyQualifiedName") != fq:
                raise RuntimeError("schema creation mismatch")
            schemas[fq] = z

    def register_table(fqn, snapshot, description):
        _, ns, name = fqn.split(".")
        register_schema(ns)
        dest = DB + "." + ns + "." + name
        observed = t.sql("SHOW COLUMNS FROM " + fqn)
        cols = [coldef(r) for r in observed]
        if not cols:
            raise RuntimeError("table has no columns")
        if dest not in tables:
            z = tool(
                c,
                "create_table",
                {
                    "table_data": {
                        "name": name,
                        "databaseSchema": DB + "." + ns,
                        "description": description,
                        "tableType": "Regular",
                        "columns": cols,
                    }
                },
            )
            if z.get("fullyQualifiedName") != dest:
                raise RuntimeError("table creation mismatch")
            tables[dest] = z
        else:
            z = tool(c, "get_table_by_name", {"fqn": dest, "fields": "owners"})
            if [v["name"] for v in z.get("columns", [])] != [v["name"] for v in cols]:
                raise RuntimeError("catalog table schema mismatch")
            tables[dest] = z
        z = tool(c, "get_table_by_name", {"fqn": dest, "fields": "owners"})
        if not z.get("owners"):
            tool(
                c,
                "update_table",
                {
                    "table_id": z["id"],
                    "operations": [
                        {
                            "op": "add",
                            "path": "/owners",
                            "value": [{"id": OPERATOR, "type": "user"}],
                        }
                    ],
                },
            )
        return tables[dest], {col["name"] for col in cols}

    assets = {}
    columns = {}
    for side in ("baseline", "delta"):
        f = x[side + "_table"]
        assets[f], columns[f] = register_table(
            f,
            x[side + "_snapshot"],
            "Фактическая входная Iceberg-таблица; snapshot "
            + str(x[side + "_snapshot"])
            + ", cut "
            + x["cut_id"]
            + ". Контракт и граница должны проверяться отдельно.",
        )
    for name in expected:
        f = "dlh." + schema + "." + name
        assets[f], columns[f] = register_table(
            f,
            e["snapshots"][name],
            "Фактическая таблица подготовленного release "
            + schema
            + "; Iceberg snapshot "
            + str(e["snapshots"][name])
            + ", cut "
            + x["cut_id"]
            + ". Синтетические данные; владелец admin — оператор стенда.",
        )

    def edge(src, dst, mapping):
        a, b = assets[src], assets[dst]
        fa = DB + "." + src.split(".", 1)[1]
        fb = DB + "." + dst.split(".", 1)[1]
        maps = []
        for target, sources in mapping.items():
            if target not in columns[dst] or any(
                v not in columns[src] for v in sources
            ):
                raise RuntimeError("lineage columns mismatch")
            maps.append(
                {
                    "fromColumns": [fa + "." + v for v in sources],
                    "toColumn": fb + "." + target,
                }
            )
        upsert_edge(
            c,
            a,
            b,
            "Ручной lineage по фактически выполненному SQL релиза "
            + schema
            + "; одноимённый столбец JOIN не обязательно является проекцией.",
            maps,
        )

    prefix = "dlh." + schema + "."
    if x.get("mode") == "initial":
        if not re.fullmatch(
            r"dlh\.oil_i_[a-z0-9_]{1,42}\.baseline_event", x["baseline_table"]
        ):
            raise ValueError("initial baseline identifier mismatch")
        gp_fqn = "og_greenplum.og_dwh.oil_dds.event"
        gp = tool(c, "get_table_by_name", {"fqn": gp_fqn})
        gp_cols = {v["name"] for v in gp.get("columns", [])}
        if not gp_cols:
            raise RuntimeError("GP source metadata missing")
        tgt_fqn = DB + "." + x["baseline_table"].split(".", 1)[1]
        mappings = [
            {"fromColumns": [gp_fqn + "." + v], "toColumn": tgt_fqn + "." + v}
            for v in sorted(gp_cols & columns[x["baseline_table"]])
        ]
        if not mappings:
            raise RuntimeError("baseline column lineage missing")
        upsert_edge(
            c,
            gp,
            assets[x["baseline_table"]],
            "Фактическая INITIAL CTAS из GP event при зафиксированной границе; полная история событий 4 типов.",
            mappings,
        )
    edge(
        x["baseline_table"],
        prefix + "canonical_input",
        {"document": ["event_document"]},
    )
    edge(x["delta_table"], prefix + "canonical_input", {"document": ["event_json"]})
    edge(
        prefix + "canonical_input",
        prefix + "canonical_event",
        {
            n: ["document"]
            for n in (
                "document",
                "event_id",
                "event_type",
                "entity_id",
                "entity_version",
                "op",
                "event_time",
            )
        },
    )
    for name, sql in sql_plan.plan(schema):
        dst = prefix + name
        deps = set(
            re.findall(
                r"\b(?:FROM|JOIN)\s+(dlh\." + re.escape(schema) + r"\.[a-z][a-z0-9_]*)",
                sql,
                re.I,
            )
        )
        for src in sorted(deps):
            # Register table-level dependencies for JOIN/filter; add only
            # columns that are unmistakable direct projections or documented calculations.
            mapping = {}
            if src == prefix + "canonical_event":
                if name.startswith("hub_"):
                    mapping = {
                        "entity_id": ["entity_id"],
                        (
                            "production_hk"
                            if name == "hub_production_day"
                            else name.replace("hub_", "") + "_hk"
                        ): ["entity_id"],
                    }
                elif name.startswith("sat_"):
                    mapping = {
                        v: [v]
                        for v in (
                            "event_id",
                            "entity_version",
                            "op",
                            "event_time",
                            "document",
                        )
                    }
                    mapping[
                        (
                            "production_hk"
                            if name == "sat_production_day"
                            else name.replace("sat_", "") + "_hk"
                        )
                    ] = ["entity_id"]
                elif name.startswith("link_"):
                    mapping = {"link_hk": ["entity_id", "document"]}
                    left, right = {
                        "link_well_field": ("well_hk", "field_hk"),
                        "link_production_day_well": ("production_hk", "well_hk"),
                        "link_downtime_well": ("downtime_hk", "well_hk"),
                    }[name]
                    mapping[left] = ["entity_id"]
                    mapping[right] = ["document"]
                elif name == "dim_field":
                    mapping = {
                        "field_hk": ["entity_id"],
                        "entity_version": ["entity_version"],
                        "valid_from": ["event_time"],
                        "valid_to": ["event_time"],
                        "field_name": ["document"],
                        "region": ["document"],
                        "is_deleted": ["op"],
                    }
                elif name == "dim_well":
                    mapping = {
                        "well_hk": ["entity_id"],
                        "field_hk": ["document"],
                        "entity_version": ["entity_version"],
                        "valid_from": ["document"],
                        "valid_to": ["document"],
                        "well_name": ["document"],
                        "status": ["document"],
                        "is_deleted": ["op"],
                    }
                elif name == "fact_well_day":
                    mapping = {
                        "production_hk": ["entity_id"],
                        "well_hk": ["document"],
                        "production_date": ["document"],
                        "oil_tonnes": ["document"],
                        "status": ["document"],
                    }
                elif name == "fact_downtime_interval":
                    mapping = {
                        "downtime_hk": ["entity_id"],
                        "well_hk": ["document"],
                        "start_at": ["document"],
                        "end_at": ["document"],
                        "reason": ["document"],
                        "hours": ["document"],
                    }
            elif (src.rsplit(".", 1)[-1], name) == ("dim_well", "fact_well_day"):
                mapping = {"field_hk": ["field_hk"], "well_version": ["entity_version"]}
            elif (src.rsplit(".", 1)[-1], name) == ("dim_field", "fact_well_day"):
                mapping = {"field_version": ["entity_version"]}
            elif (src.rsplit(".", 1)[-1], name) == (
                "fact_well_day",
                "fact_downtime_interval",
            ):
                mapping = {"field_hk": ["field_hk"]}
            elif (src.rsplit(".", 1)[-1], name) == ("fact_well_day", "agg_field_day"):
                mapping = {
                    "production_date": ["production_date"],
                    "field_hk": ["field_hk"],
                    "oil_tonnes": ["oil_tonnes"],
                    "producing_well_days": ["status"],
                    "oil_per_well_day": ["oil_tonnes", "status"],
                }
            elif (src.rsplit(".", 1)[-1], name) == ("dim_field", "agg_field_day"):
                mapping = {"field_name": ["field_name"], "region": ["region"]}
            elif (src.rsplit(".", 1)[-1], name) == (
                "agg_field_day",
                "field_performance",
            ):
                mapping = {n: [n] for n in columns[src] & columns[dst]}
            elif (src.rsplit(".", 1)[-1], name) == (
                "agg_field_downtime_day",
                "field_performance",
            ):
                mapping = {"downtime_hours": ["downtime_hours"]}
            elif (src.rsplit(".", 1)[-1], name) == (
                "fact_downtime_interval",
                "agg_field_downtime_day",
            ):
                mapping = {
                    "production_date": ["start_at"],
                    "field_hk": ["field_hk"],
                    "downtime_hours": ["hours"],
                }
            edge(src, dst, mapping)
    if a.phase == "published":
        # Verify underlying snapshots and dataset binding again immediately prior to mutation.
        superseted_binding(schema, a.dataset_id)
        if snap(t, prefix + "field_performance") != e["snapshots"]["field_performance"]:
            raise ValueError("published snapshot changed")
        published = assets[prefix + "field_performance"]
        for fqn in CHARTS:
            chart = tool(c, "get_chart_by_name", {"fqn": fqn})
            graph = tool(
                c,
                "get_lineage",
                {
                    "entity_id": chart["id"],
                    "entity_type": "chart",
                    "upstream_depth": 1,
                    "downstream_depth": 0,
                },
            )
            nodes = {v["id"]: v for v in graph.get("nodes", [])}
            obsolete = []
            for item in graph.get("upstreamEdges", []):
                if item.get("toEntity") != chart["id"]:
                    continue
                node = nodes.get(item.get("fromEntity"), {})
                name = node.get("fullyQualifiedName", "")
                if name == TEMPLATE or re.fullmatch(
                    r"dlh_trino\.dlh\.oil_r_[a-z0-9_]{1,42}\.field_performance", name
                ):
                    if node.get("id") != published["id"]:
                        obsolete.append(node["id"])
            # Add the confirmed release first. Remove only previous demo-serving
            # release/template edges; preserve unrelated human-curated lineage.
            tool(
                c,
                "add_lineage",
                {
                    "lineage_data": {
                        "edge": {
                            "fromEntity": {"id": published["id"], "type": "table"},
                            "toEntity": {"id": chart["id"], "type": "chart"},
                            "lineageDetails": {
                                "source": "Manual",
                                "description": "Dataset 8 binding verified against release field_performance after publication.",
                            },
                        }
                    }
                },
            )
            for old_id in obsolete:
                tool(
                    c,
                    "delete_lineage_by_id",
                    {
                        "from_type": "table",
                        "from_id": old_id,
                        "to_type": "chart",
                        "to_id": chart["id"],
                    },
                )
            check = tool(
                c,
                "get_lineage",
                {
                    "entity_id": chart["id"],
                    "entity_type": "chart",
                    "upstream_depth": 1,
                    "downstream_depth": 0,
                },
            )
            current = {
                v.get("fromEntity")
                for v in check.get("upstreamEdges", [])
                if v.get("toEntity") == chart["id"]
            }
            if published["id"] not in current or any(v in current for v in obsolete):
                raise RuntimeError("chart lineage readback failed")
            tool(
                c,
                "set_demo_bi_description",
                {
                    "entity_type": "chart",
                    "entity_id": chart["id"],
                    "description": "График фактического Superset dataset 8; после проверенного переключения читает dlh."
                    + schema
                    + ".field_performance. Данные синтетические; состояние релиза подтверждается evidence.",
                },
            )
        dashboard = tool(
            c, "get_dashboard_by_name", {"fqn": "webinar_superset.dashboard_6"}
        )
        tool(
            c,
            "set_demo_bi_description",
            {
                "entity_type": "dashboard",
                "entity_id": dashboard["id"],
                "description": "Панель Superset 6: dataset 8 после проверенного переключения читает dlh."
                + schema
                + ".field_performance. Данные синтетические; назначение среза и статус приёмки следует проверять по release evidence.",
            },
        )
    print(
        json.dumps(
            {
                "status": (
                    "catalog_ready" if a.phase == "ready" else "catalog_published"
                ),
                "schema": schema,
                "release_objects": len(expected),
            },
            ensure_ascii=False,
        )
    )


if __name__ == "__main__":
    main()
