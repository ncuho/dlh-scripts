# Скрипт принимает ready evidence через stdin внутри штатного token proxy и назначает существующей CIAM-backed роли oil_dlh_reader точечный SELECT. Для production — agg_field_day, для performance — agg_field_day и field_performance. UUID и sn.
# Права не копируются SQL GRANT из GP; новый UUID выпуска требует нового назначения и проверки.
"""Run inside LakeKeeper M2M proxy. Grant reader SELECT only on two release tables.

Requires a validated ready evidence JSON on stdin, fixes role and warehouse IDs,
never mutates OpenFGA directly and never prints bearer tokens.
"""

import json, re, sys, urllib.request, urllib.error

x = json.load(sys.stdin)
schema = x.get("schema", "")
snapshots = x.get("snapshots", {})
if x.get("status") != "ready" or not re.fullmatch(r"oil_r_[a-z0-9_]{1,42}", schema):
    raise SystemExit("release not ready or invalid schema")
serving = ("agg_field_day",) if x.get("chain") == "production" else ("agg_field_day", "field_performance")
for n in serving:
    if type(snapshots.get(n)) is not int or snapshots[n] <= 0:
        raise SystemExit("missing serving snapshot")
role = "01a1fffd-bdc0-7c31-8738-c6d32c823dc7"
w = "be4599ce-dbba-11f1-a520-7382628b06be"
base = "http://lakekeeper-service:8181"
opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
with opener.open(
    urllib.request.Request(
        "http://127.0.0.1:8080/token",
        data=b"grant_type=client_credentials",
        headers={"Content-Type": "application/x-www-form-urlencoded"},
    ),
    timeout=10,
) as r:
    token = json.load(r)["access_token"]


def req(path, body=None):
    """Отправляет авторизованный запрос LakeKeeper и возвращает статус с результатом."""
    h = {"Authorization": "Bearer " + token}
    if body is not None:
        h["Content-Type"] = "application/json"
        body = json.dumps(body).encode()
    try:
        with opener.open(
            urllib.request.Request(base + path, data=body, headers=h), timeout=20
        ) as r:
            raw = r.read()
            return r.status, json.loads(raw) if raw else {}
    except urllib.error.HTTPError as e:
        return e.code, {}


results = []
for name in serving:
    status, d = req(f"/catalog/v1/{w}/namespaces/{schema}/tables/{name}")
    if (
        status != 200
        or d.get("metadata", {}).get("current-snapshot-id") != snapshots[name]
    ):
        raise SystemExit("serving table snapshot differs")
    tid = d["metadata"]["table-uuid"]
    path = f"/management/v1/permissions/warehouse/{w}/table/{tid}"
    status, d = req(path + "/assignments")
    if status != 200:
        raise SystemExit("cannot inspect assignments")
    exists = any(
        a.get("role") == role and a.get("type") == "select"
        for a in d.get("assignments", [])
    )
    if not exists:
        status, _ = req(
            path + "/assignments",
            {"writes": [{"role": role, "type": "select"}], "deletes": []},
        )
        if status not in (200, 204):
            raise SystemExit("grant failed; inspect before retry")
    status, d = req(path + "/assignments")
    if status != 200 or not any(
        a.get("role") == role and a.get("type") == "select"
        for a in d.get("assignments", [])
    ):
        raise SystemExit("grant readback failed")
    results.append(
        {
            "table": name,
            "uuid": tid,
            "role": role,
            "relation": "select",
            "already_existed": exists,
            "snapshot_id": snapshots[name],
        }
    )
print(
    json.dumps(
        {
            "schema": schema,
            "serving_select_verified": True,
            "role": role,
            "tables": results,
        }
    ),
    flush=True,
)
