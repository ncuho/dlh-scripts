"""Run inside the existing management proxy pod. Add scoped CIAM-backed roles.

Uses the same management assignment API as LakeKeeper UI, never raw OpenFGA.
No existing assignment is removed; no runtime credential is returned.
"""

import json
import urllib.request

BASE = "http://lakekeeper-service:8181"
WAREHOUSE = "be4599ce-dbba-11f1-a520-7382628b06be"
op = urllib.request.build_opener(urllib.request.ProxyHandler({}))
with op.open(
    urllib.request.Request(
        "http://127.0.0.1:8080/token",
        data=b"grant_type=client_credentials",
        headers={"Content-Type": "application/x-www-form-urlencoded"},
    ),
    timeout=15,
) as r:
    token = json.load(r)["access_token"]


def api(path, method="GET", body=None):
    request = urllib.request.Request(
        BASE + path,
        data=json.dumps(body).encode() if body is not None else None,
        headers={
            "Authorization": "Bearer " + token,
            "Content-Type": "application/json",
        },
        method=method,
    )
    with op.open(request, timeout=25) as r:
        return json.loads(r.read() or b"{}")


roles = api("/management/v1/role")["roles"]
result = {"role_source": "CIAM role codes", "assignments": []}
for name in ["oil_dlh_reader", "oil_dlh_engineer"]:
    match = [r for r in roles if r.get("source-id") == name]
    if not match:
        match = [
            api(
                "/management/v1/role",
                "POST",
                {
                    "name": name,
                    "source-id": name,
                    "description": (
                        "Чтение опубликованных витрин нефтедобычи."
                        if name.endswith("reader")
                        else "Обработка приёма, истории и витрин нефтедобычи."
                    ),
                },
            )
        ]
    if len(match) != 1:
        raise RuntimeError("ambiguous source role " + name)
    role = match[0]["id"]
    description = (
        "Чтение опубликованных витрин нефтедобычи."
        if name.endswith("reader")
        else "Обработка приёма, истории и витрин нефтедобычи."
    )
    if match[0].get("description") != description:
        api(
            "/management/v1/role/" + role,
            "POST",
            {"name": name, "source-id": name, "description": description},
        )
    scopes = [("warehouse/" + WAREHOUSE, ["describe"])]
    for namespace in (
        ["oil_mart"] if name.endswith("reader") else ["oil_stg", "oil_dds", "oil_mart"]
    ):
        ns = api("/catalog/v1/" + WAREHOUSE + "/namespaces/" + namespace)
        namespace_id = ns["properties"]["namespace_id"]
        scopes.append(
            (
                "namespace/" + namespace_id,
                (
                    ["describe", "select"]
                    if name.endswith("reader")
                    else ["describe", "select", "create", "modify"]
                ),
            )
        )
    for scope, rights in scopes:
        path = "/management/v1/permissions/" + scope + "/assignments"
        before = api(path)["assignments"]
        writes = [
            {"role": role, "type": right}
            for right in rights
            if not any(a.get("role") == role and a.get("type") == right for a in before)
        ]
        if writes:
            api(path, "POST", {"writes": writes})
        after = api(path)["assignments"]
        for right in rights:
            if not any(a.get("role") == role and a.get("type") == right for a in after):
                raise RuntimeError("assignment readback failed")
        result["assignments"].append(
            {"role_name": name, "role_id": role, "scope": scope, "rights": rights}
        )
print(json.dumps(result, indent=2))
