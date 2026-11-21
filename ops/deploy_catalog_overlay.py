"""Deploy persistent ConfigMap overlay to current deployment; chart source must also adopt it."""

import hashlib, json, subprocess
from pathlib import Path

SSH = ["ssh", "-o", "BatchMode=yes", "redos@79.137.175.114"]
NS = "datacatalogmcpcluster-b5tvpxw7"
DEP = "data-catalog-mcp"
CM = "oil-catalog-lineage-overlay"


def run(cmd, data=None):
    p = subprocess.run(
        SSH + ["kubectl"] + cmd, input=data, text=True, capture_output=True, timeout=90
    )
    if p.returncode:
        raise RuntimeError(
            "kubectl operation failed; inspect deployment status without printing secrets"
        )
    return p.stdout


x = json.loads(run(["get", "deployment", DEP, "-n", NS, "-o", "json"]))
s = x["spec"]["template"]["spec"]
cs = s["containers"]
if len(cs) != 1 or cs[0]["name"] != "data-catalog-mcp":
    raise RuntimeError("unexpected deployment layout")
content = Path(__file__).with_name("catalog_launcher_overlay.py").read_text()
digest = hashlib.sha256(content.encode()).hexdigest()
cm = {
    "apiVersion": "v1",
    "kind": "ConfigMap",
    "metadata": {
        "name": CM,
        "namespace": NS,
        "labels": {"app.kubernetes.io/part-of": "oil-webinar"},
    },
    "data": {"openmetadata_launcher.py": content},
}
run(["apply", "-f", "-"], json.dumps(cm))
volumes = [
    v for v in s.get("volumes", []) if v["name"] != "oil-catalog-lineage-overlay"
]
volumes.append({"name": "oil-catalog-lineage-overlay", "configMap": {"name": CM}})
mounts = [
    v
    for v in cs[0].get("volumeMounts", [])
    if v.get("mountPath") != "/app/openmetadata_launcher.py"
]
mounts.append(
    {
        "name": "oil-catalog-lineage-overlay",
        "mountPath": "/app/openmetadata_launcher.py",
        "subPath": "openmetadata_launcher.py",
        "readOnly": True,
    }
)
patch = {
    "spec": {
        "template": {
            "metadata": {"annotations": {"oil-webinar/catalog-overlay-sha256": digest}},
            "spec": {
                "volumes": volumes,
                "containers": [{"name": "data-catalog-mcp", "volumeMounts": mounts}],
            },
        }
    }
}
run(
    [
        "patch",
        "deployment",
        DEP,
        "-n",
        NS,
        "--type=strategic",
        "--patch-file",
        "/dev/stdin",
    ],
    json.dumps(patch),
)
run(["rollout", "status", "deployment/" + DEP, "-n", NS, "--timeout=90s"])
print("catalog overlay rolled out; sha256", digest)
