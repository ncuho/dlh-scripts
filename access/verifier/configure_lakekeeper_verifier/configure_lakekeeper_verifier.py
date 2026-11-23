# Контекст миграции: AS IS / TO BE.
# Назначение: Разворачивает verifier с TLS, авторизацией и ограничением сетевого доступа. Секреты хранятся в Kubernetes Secret.
# AS IS: Source-доступ осуществляется через GP, TLS и зарегистрированные identities.
# TO BE: Настраивает проверяемый транспорт/доверие или verifier target-сервисов.
# Отличие: Это инфраструктурный доступ, а не автоматический перенос бизнес-ролей; TLS не отключается ради миграции.
# Конец контекста миграции.
"""Configure a narrowly scoped, read-only serving verifier beside LK token proxy."""

import json
import subprocess
from pathlib import Path

NS = "lakekeepercluster-dqs9wggw"
MCP = "trinomcpcluster-xmrnjrpc"
DEPLOY = "lakekeeper-m2m-token-proxy"
SERVICE = "oil-lakekeeper-verifier"
LABELS = {
    "app.kubernetes.io/name": DEPLOY,
    "app.kubernetes.io/component": "m2m-token-proxy",
}
SSH = ["ssh", "-o", "BatchMode=yes", "redos@79.137.175.114"]


def kubectl(args, obj=None, timeout=45):
    """Выполняет фиксированную Kubernetes операцию без вывода значений Secret."""
    process = subprocess.run(
        SSH + ["kubectl"] + args,
        input=json.dumps(obj) if obj is not None else None,
        text=True,
        capture_output=True,
        timeout=timeout,
    )
    if process.returncode:
        raise RuntimeError("kubectl failed: " + " ".join(args[:3]))
    return process.stdout


def apply(obj):
    """Применяет декларативное описание сервисного ресурса и проверяет ответ."""
    kubectl(["apply", "-f", "-"], obj)


def main():
    """Точка входа: Configure a narrowly scoped, read-only serving verifier beside LK token proxy. Импорт модуля сам по себе эту операцию не запускает."""
    deployment = json.loads(
        kubectl(["get", "deployment", DEPLOY, "-n", NS, "-o", "json"])
    )
    spec = deployment["spec"]["template"]["spec"]
    images = [c["image"] for c in spec["containers"] if c["name"] == "m2m-token-proxy"]
    if len(images) != 1:
        raise RuntimeError("unexpected token proxy deployment")
    labels = deployment["spec"]["template"]["metadata"].get("labels", {})
    if any(labels.get(k) != v for k, v in LABELS.items()):
        raise RuntimeError("unexpected deployment labels")
    policies = json.loads(kubectl(["get", "networkpolicy", "-n", NS, "-o", "json"]))[
        "items"
    ]
    if any(p["metadata"]["name"] != SERVICE for p in policies):
        raise RuntimeError(
            "unexpected existing policy; inspect before changing ingress"
        )
    # Copy only this existing bounded gateway token in memory; never print it.
    source = json.loads(
        kubectl(["get", "secret", "oil-runtime-gateway-auth", "-n", MCP, "-o", "json"])
    )
    encoded_token = source.get("data", {}).get("token")
    if not isinstance(encoded_token, str) or not encoded_token:
        raise RuntimeError("runtime token absent")
    apply(
        {
            "apiVersion": "v1",
            "kind": "Secret",
            "metadata": {"name": "oil-lakekeeper-verifier-auth", "namespace": NS},
            "type": "Opaque",
            "data": {"token": encoded_token},
        }
    )
    apply(
        {
            "apiVersion": "cert-manager.io/v1",
            "kind": "Certificate",
            "metadata": {"name": "oil-lakekeeper-verifier-tls", "namespace": NS},
            "spec": {
                "secretName": "oil-lakekeeper-verifier-tls",
                "duration": "720h",
                "renewBefore": "168h",
                "issuerRef": {
                    "name": "self-signed-local-issuer",
                    "kind": "ClusterIssuer",
                    "group": "cert-manager.io",
                },
                "dnsNames": [
                    SERVICE + "." + NS + ".svc",
                    SERVICE + "." + NS + ".svc.cluster.local",
                ],
            },
        }
    )
    kubectl(
        [
            "wait",
            "certificate/oil-lakekeeper-verifier-tls",
            "-n",
            NS,
            "--for=condition=Ready",
            "--timeout=90s",
        ],
        timeout=100,
    )
    code = (
        Path(__file__).resolve().parents[1]
        / "lakekeeper_verifier"
        / "lakekeeper_verifier.py"
    ).read_text()
    apply(
        {
            "apiVersion": "v1",
            "kind": "ConfigMap",
            "metadata": {"name": SERVICE, "namespace": NS},
            "data": {"server.py": code},
        }
    )
    # No policies existed before installation.  Preserve previously unrestricted
    # ingress to 8080 while admitting 8091 only from the MCP namespace.
    apply(
        {
            "apiVersion": "networking.k8s.io/v1",
            "kind": "NetworkPolicy",
            "metadata": {"name": SERVICE, "namespace": NS},
            "spec": {
                "podSelector": {"matchLabels": LABELS},
                "policyTypes": ["Ingress"],
                "ingress": [
                    {"ports": [{"protocol": "TCP", "port": 8080}]},
                    {
                        "from": [
                            {
                                "namespaceSelector": {
                                    "matchLabels": {"kubernetes.io/metadata.name": MCP}
                                }
                            }
                        ],
                        "ports": [{"protocol": "TCP", "port": 8091}],
                    },
                ],
            },
        }
    )
    # The platform's Calico global allow at order 200 precedes Kubernetes
    # NetworkPolicy. A narrow earlier Calico policy is required in this cluster.
    # It touches only this pod and these two TCP ports.
    apply(
        {
            "apiVersion": "crd.projectcalico.org/v1",
            "kind": "NetworkPolicy",
            "metadata": {"name": SERVICE + "-port-guard", "namespace": NS},
            "spec": {
                "order": 98,
                "types": ["Ingress"],
                "selector": "app.kubernetes.io/name == 'lakekeeper-m2m-token-proxy' && app.kubernetes.io/component == 'm2m-token-proxy'",
                "ingress": [
                    {
                        "action": "Allow",
                        "protocol": "TCP",
                        "source": {
                            "namespaceSelector": "kubernetes.io/metadata.name == 'trinomcpcluster-xmrnjrpc'"
                        },
                        "destination": {"ports": [8091]},
                    },
                    {
                        "action": "Deny",
                        "protocol": "TCP",
                        "destination": {"ports": [8091]},
                    },
                    {
                        "action": "Allow",
                        "protocol": "TCP",
                        "destination": {"ports": [8080]},
                    },
                ],
            },
        }
    )
    apply(
        {
            "apiVersion": "v1",
            "kind": "Service",
            "metadata": {"name": SERVICE, "namespace": NS},
            "spec": {
                "selector": LABELS,
                "ports": [
                    {
                        "name": "verify",
                        "port": 8091,
                        "targetPort": 8091,
                        "protocol": "TCP",
                    }
                ],
            },
        }
    )
    patch = {
        "spec": {
            "strategy": {
                "type": "RollingUpdate",
                "rollingUpdate": {"maxUnavailable": 0, "maxSurge": 1},
            },
            "template": {
                "spec": {
                    "volumes": [
                        {
                            "name": SERVICE,
                            "configMap": {"name": SERVICE, "defaultMode": 292},
                        },
                        {
                            "name": "oil-verifier-tls",
                            "secret": {
                                "secretName": "oil-lakekeeper-verifier-tls",
                                "defaultMode": 292,
                            },
                        },
                        {
                            "name": "oil-verifier-auth",
                            "secret": {
                                "secretName": "oil-lakekeeper-verifier-auth",
                                "defaultMode": 292,
                            },
                        },
                    ],
                    "containers": [
                        {
                            "name": SERVICE,
                            "image": images[0],
                            "command": ["python3", "-u", "/opt/oil-verifier/server.py"],
                            "ports": [
                                {
                                    "name": "verify",
                                    "containerPort": 8091,
                                    "protocol": "TCP",
                                }
                            ],
                            "volumeMounts": [
                                {
                                    "name": SERVICE,
                                    "mountPath": "/opt/oil-verifier",
                                    "readOnly": True,
                                },
                                {
                                    "name": "oil-verifier-tls",
                                    "mountPath": "/opt/oil-verifier-tls",
                                    "readOnly": True,
                                },
                                {
                                    "name": "oil-verifier-auth",
                                    "mountPath": "/opt/oil-verifier-auth",
                                    "readOnly": True,
                                },
                            ],
                            "resources": {
                                "requests": {"cpu": "20m", "memory": "32Mi"},
                                "limits": {"cpu": "200m", "memory": "128Mi"},
                            },
                            "securityContext": {
                                "runAsNonRoot": True,
                                "runAsUser": 10001,
                                "allowPrivilegeEscalation": False,
                                "readOnlyRootFilesystem": True,
                                "capabilities": {"drop": ["ALL"]},
                                "seccompProfile": {"type": "RuntimeDefault"},
                            },
                            "readinessProbe": {
                                "httpGet": {
                                    "path": "/healthz",
                                    "port": 8091,
                                    "scheme": "HTTPS",
                                },
                                "periodSeconds": 5,
                            },
                            "livenessProbe": {
                                "httpGet": {
                                    "path": "/healthz",
                                    "port": 8091,
                                    "scheme": "HTTPS",
                                },
                                "periodSeconds": 15,
                            },
                        }
                    ],
                }
            },
        }
    }
    kubectl(
        [
            "patch",
            "deployment",
            DEPLOY,
            "-n",
            NS,
            "--type=strategic",
            "--patch-file",
            "/dev/stdin",
        ],
        patch,
    )
    kubectl(
        ["rollout", "status", "deployment/" + DEPLOY, "-n", NS, "--timeout=120s"],
        timeout=140,
    )
    print(
        json.dumps(
            {
                "status": "configured",
                "service_dns": SERVICE + "." + NS + ".svc.cluster.local",
                "port": 8091,
            }
        )
    )


if __name__ == "__main__":
    main()
