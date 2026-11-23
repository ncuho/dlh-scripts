# Контекст миграции: AS IS / TO BE.
# Назначение: Переключает только разрешённый Superset dataset при совпадении прежней привязки. Проверяет новый запрос и выполняет rollback при ошибке.
# AS IS: Исходный Superset читает GP dataset и dashboard.
# TO BE: Публикуется отдельный target dataset/charts/dashboard через Cedrus.
# Отличие: Source BI не перезаписывается; перед publication проверяются chart queries, grants и release.
# Конец контекста миграции.
"""Guarded Superset dataset switch. Default dry-run; --apply requires ready evidence.

Token file and TLS CA come from environment, never command arguments. Operator must
hold the publication fence; Superset does not provide compare-and-swap here.
"""

import argparse, json, os, re, ssl, urllib.request, urllib.error, http.cookiejar
from pathlib import Path
import sql_plan
from cedrus_release_runner import MCP, snap


def main():
    """Точка входа: Guarded Superset dataset switch. Default dry-run; --apply requires ready evidence. Импорт модуля сам по себе эту операцию не запускает."""
    p = argparse.ArgumentParser()
    p.add_argument("--evidence", required=True)
    p.add_argument("--grant-evidence", required=True)
    p.add_argument("--dataset-id", required=True, type=int)
    p.add_argument("--expected-schema", required=True)
    p.add_argument("--expected-table", required=True)
    p.add_argument(
        "--table", required=True, choices=["agg_field_day", "field_performance"]
    )
    p.add_argument("--database-id", required=True, type=int)
    p.add_argument("--apply", action="store_true")
    a = p.parse_args()
    e = json.loads(Path(a.evidence).read_text())
    g = json.loads(Path(a.grant_evidence).read_text())
    schema = e.get("schema", "")
    if (
        e.get("status") != "ready"
        or not sql_plan.SCHEMA.fullmatch(schema)
        or type(e.get("snapshots", {}).get(a.table)) is not int
    ):
        raise ValueError("release is not ready")
    if (
        g.get("schema") != schema
        or g.get("serving_select_verified") is not True
        or g.get("role") != "01a1fffd-bdc0-7c31-8738-c6d32c823dc7"
    ):
        raise ValueError("serving role is not verified")
    matches = [
        t
        for t in g.get("tables", [])
        if t.get("table") == a.table
        and t.get("snapshot_id") == e["snapshots"][a.table]
        and t.get("relation") == "select"
    ]
    if len(matches) != 1:
        raise ValueError("serving grant evidence mismatch")
    m = MCP(
        os.environ.get("OIL_TRINO_MCP_URL", "http://127.0.0.1:8000/mcp"),
        os.environ.get("OIL_MCP_CA_FILE"),
        os.environ.get("OIL_MCP_BEARER_FILE"),
    )
    if snap(m, "dlh." + schema + "." + a.table) != e["snapshots"][a.table]:
        raise ValueError("serving snapshot changed")
    if not re.fullmatch(r"[a-z][a-z0-9_]{0,62}", a.expected_schema) or not re.fullmatch(
        r"[a-z][a-z0-9_]{0,62}", a.expected_table
    ):
        raise ValueError("invalid expected binding")
    url = os.environ["OIL_SUPERSET_URL"].rstrip("/")
    if not url.startswith("https://"):
        raise ValueError("Superset URL must use HTTPS")
    tokenfile = Path(os.environ["OIL_SUPERSET_BEARER_FILE"])
    ca = os.environ.get("OIL_SUPERSET_CA_FILE")
    ctx = ssl.create_default_context(cafile=ca)
    jar = http.cookiejar.CookieJar()
    client = urllib.request.build_opener(
        urllib.request.HTTPSHandler(context=ctx),
        urllib.request.HTTPCookieProcessor(jar),
    )

    def call(path, method="GET", data=None, csrf=None):
        """Выполняет один запрос к сервису или провайдеру с явно заданным контекстом и проверкой ответа."""
        h = {
            "Accept": "application/json",
            "Content-Type": "application/json",
            "Authorization": "Bearer " + tokenfile.read_text().strip(),
        }
        if csrf:
            h["X-CSRFToken"] = csrf
            h["Referer"] = url + "/"
        req = urllib.request.Request(
            url + path,
            data=json.dumps(data).encode() if data is not None else None,
            headers=h,
            method=method,
        )
        with client.open(req, timeout=40) as r:
            raw = r.read()
            return r.status, json.loads(raw) if raw else {}

    endpoint = "/api/v1/dataset/" + str(a.dataset_id)
    _, current = call(endpoint)
    v = current["result"]
    old = {"schema": v["schema"], "table_name": v["table_name"]}
    if v["database"]["id"] != a.database_id or old != {
        "schema": a.expected_schema,
        "table_name": a.expected_table,
    }:
        raise ValueError("dataset binding changed; stop")
    if not a.apply:
        print(
            json.dumps(
                {
                    "status": "dry_run",
                    "dataset_id": a.dataset_id,
                    "current": old,
                    "proposed": {"schema": schema, "table_name": a.table},
                }
            )
        )
        return 0
    _, csrfbody = call("/api/v1/security/csrf_token/")
    csrf = csrfbody["result"]
    switch_attempted = False
    try:
        switch_attempted = True
        status, _ = call(
            endpoint, "PUT", {"schema": schema, "table_name": a.table}, csrf
        )
        if status != 200:
            raise RuntimeError("dataset switch failed")
        _, read = call(endpoint)
        v = read["result"]
        if (v["schema"], v["table_name"]) != (schema, a.table):
            raise RuntimeError("switch readback mismatch")
        # A read through the actual dataset enforces access and catches stale/broken bindings.
        payload = {
            "datasource": {"id": a.dataset_id, "type": "table"},
            "force": True,
            "queries": [
                {
                    "filters": [],
                    "extras": {},
                    "metrics": ["count"],
                    "columns": [],
                    "row_limit": 1,
                }
            ],
            "result_format": "json",
            "result_type": "full",
            "custom_cache_timeout": -1,
        }
        status, data = call("/api/v1/chart/data", "POST", payload, csrf)
        if status != 200 or data["result"][0].get("status") != "success":
            raise RuntimeError("dataset read failed")
        print(
            json.dumps(
                {
                    "status": "switched",
                    "dataset_id": a.dataset_id,
                    "old": old,
                    "new": {"schema": schema, "table_name": a.table},
                }
            )
        )
        return 0
    except Exception:
        if switch_attempted:
            try:
                status, _ = call(endpoint, "PUT", old, csrf)
                _, read = call(endpoint)
                if status != 200 or (
                    read["result"]["schema"],
                    read["result"]["table_name"],
                ) != (old["schema"], old["table_name"]):
                    raise RuntimeError("rollback readback mismatch")
                print(
                    json.dumps(
                        {
                            "status": "switch_failed_rolled_back",
                            "dataset_id": a.dataset_id,
                        }
                    )
                )
            except Exception:
                print(
                    json.dumps(
                        {"status": "rollback_unconfirmed", "dataset_id": a.dataset_id}
                    )
                )
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
