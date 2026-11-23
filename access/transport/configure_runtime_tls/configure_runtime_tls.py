"""Configure a native cert-manager TLS/auth gateway for the migration runtime.

The opaque transport token is generated once and stays only in Kubernetes Secrets.
The existing MCP endpoint is unchanged. This adds a scoped, authenticated runtime
route for Airflow; it does not change Cedrus/LakeKeeper authorization.
"""

import base64
import json
import secrets
import subprocess
from pathlib import Path

SSH = ["ssh", "-o", "BatchMode=yes", "redos@79.137.175.114"]
NS = "trinomcpcluster-xmrnjrpc"
AF = "airflow-atomcluster-4jn2dw24"


def kubectl(args, body=None, check=True):
    """Выполняет фиксированную Kubernetes операцию без вывода значений Secret."""
    r = subprocess.run(
        SSH + ["kubectl"] + args,
        input=json.dumps(body) if body is not None else None,
        text=True,
        capture_output=True,
        timeout=40,
    )
    if check and r.returncode:
        raise RuntimeError("kubectl operation failed: " + args[0])
    return r


def apply(body):
    """Применяет декларативное описание сервисного ресурса и проверяет ответ."""
    kubectl(["apply", "-f", "-"], body)


def main():
    """Точка входа: Configure a native cert-manager TLS/auth gateway for the migration runtime. Импорт модуля сам по себе эту операцию не запускает."""
    source = json.loads(
        kubectl(
            ["get", "cm", "vkcloud", "-n", "lakekeepercluster-dqs9wggw", "-o", "json"]
        ).stdout
    )
    ca = source["data"]["root-certs.pem"]
    prior = kubectl(
        ["get", "secret", "oil-runtime-gateway-auth", "-n", NS, "-o", "json"],
        check=False,
    )
    token = (
        base64.b64decode(json.loads(prior.stdout)["data"]["token"]).decode()
        if prior.returncode == 0
        else secrets.token_urlsafe(48)
    )
    auth = (
        'map $http_authorization $oil_runtime_authorized { default 0; "Bearer '
        + token
        + '" 1; }\n'
    )
    apply(
        {
            "apiVersion": "v1",
            "kind": "Secret",
            "metadata": {"name": "oil-runtime-gateway-auth", "namespace": NS},
            "type": "Opaque",
            "data": {
                "token": base64.b64encode(token.encode()).decode(),
                "auth.conf": base64.b64encode(auth.encode()).decode(),
            },
        }
    )
    apply(
        {
            "apiVersion": "v1",
            "kind": "Secret",
            "metadata": {"name": "oil-wave2-mcp-runtime", "namespace": AF},
            "type": "Opaque",
            "data": {
                "token": base64.b64encode(token.encode()).decode(),
                "ca.crt": base64.b64encode(ca.encode()).decode(),
            },
        }
    )
    apply(
        {
            "apiVersion": "cert-manager.io/v1",
            "kind": "Certificate",
            "metadata": {"name": "oil-runtime-mcp-tls", "namespace": NS},
            "spec": {
                "secretName": "oil-runtime-mcp-tls",
                "duration": "720h",
                "renewBefore": "168h",
                "issuerRef": {
                    "name": "self-signed-local-issuer",
                    "kind": "ClusterIssuer",
                    "group": "cert-manager.io",
                },
                "dnsNames": [
                    "trino-mcp." + NS + ".svc.cluster.local",
                    "trino-mcp." + NS + ".svc",
                ],
            },
        }
    )
    config = """pid /tmp/nginx.pid;
error_log /dev/stderr warn;
events { worker_connections 256; }
http {
  access_log off;
  map_hash_bucket_size 128;
  client_body_temp_path /tmp/client_body;
  proxy_temp_path /tmp/proxy;
  fastcgi_temp_path /tmp/fastcgi;
  include /etc/oil-runtime-auth/auth.conf;
  server {
    listen 8443 ssl;
    ssl_certificate /etc/oil-runtime-tls/tls.crt;
    ssl_certificate_key /etc/oil-runtime-tls/tls.key;
    ssl_protocols TLSv1.2 TLSv1.3;
    location = /healthz { return 200 'ok'; }
    location /mcp {
      if ($oil_runtime_authorized = 0) { return 401; }
      proxy_pass http://127.0.0.1:8000;
      proxy_http_version 1.1;
      proxy_set_header Host 127.0.0.1:8000;
      proxy_set_header Connection '';
      proxy_set_header Authorization '';
      proxy_buffering off;
      proxy_connect_timeout 10s;
      proxy_read_timeout 180s;
      proxy_send_timeout 180s;
    }
    location = /catalog-mcp {
      if ($oil_runtime_authorized = 0) { return 401; }
      proxy_pass http://data-catalog-mcp.datacatalogmcpcluster-b5tvpxw7.svc.cluster.local:8000/mcp;
      proxy_http_version 1.1;
      proxy_set_header Host 127.0.0.1:8000;
      proxy_set_header Connection '';
      proxy_set_header Authorization '';
      proxy_buffering off;
      proxy_read_timeout 180s;
      proxy_send_timeout 180s;
    }
    location = /verify-serving {
      if ($oil_runtime_authorized = 0) { return 401; }
      client_max_body_size 1m;
      proxy_pass https://oil-lakekeeper-verifier.lakekeepercluster-dqs9wggw.svc.cluster.local:8091/verify;
      proxy_ssl_server_name on;
      proxy_ssl_name oil-lakekeeper-verifier.lakekeepercluster-dqs9wggw.svc.cluster.local;
      proxy_ssl_verify on;
      proxy_ssl_trusted_certificate /etc/oil-runtime-gateway/root-certs.pem;
      proxy_read_timeout 90s;
    }
    location / { return 404; }
  }
}
"""
    apply(
        {
            "apiVersion": "v1",
            "kind": "ConfigMap",
            "metadata": {"name": "oil-runtime-gateway", "namespace": NS},
            "data": {"nginx.conf": config, "root-certs.pem": ca},
        }
    )
    image = "dp-repo-cp.dp-103.mcs.local/infra/ingress-nginx/controller:v1.15.1@sha256:de8fd8f1def14ed36f59c9838bd3c920ecb01754072e3e9f10832d62ef876cb8"
    sidecar = {
        "name": "oil-runtime-tls",
        "image": image,
        "command": [
            "/usr/local/nginx/sbin/nginx",
            "-c",
            "/etc/oil-runtime-gateway/nginx.conf",
            "-g",
            "daemon off;",
        ],
        "ports": [{"name": "runtime-tls", "containerPort": 8443}],
        "resources": {
            "requests": {"cpu": "25m", "memory": "32Mi"},
            "limits": {"cpu": "250m", "memory": "128Mi"},
        },
        "securityContext": {
            "runAsUser": 101,
            "runAsNonRoot": True,
            "allowPrivilegeEscalation": False,
            "readOnlyRootFilesystem": True,
            "capabilities": {"drop": ["ALL"], "add": ["NET_BIND_SERVICE"]},
        },
        "readinessProbe": {
            "httpGet": {"path": "/healthz", "port": 8443, "scheme": "HTTPS"},
            "periodSeconds": 5,
        },
        "volumeMounts": [
            {
                "name": "oil-runtime-gateway",
                "mountPath": "/etc/oil-runtime-gateway",
                "readOnly": True,
            },
            {
                "name": "oil-runtime-auth",
                "mountPath": "/etc/oil-runtime-auth",
                "readOnly": True,
            },
            {
                "name": "oil-runtime-tls",
                "mountPath": "/etc/oil-runtime-tls",
                "readOnly": True,
            },
            {"name": "oil-runtime-tmp", "mountPath": "/tmp"},
        ],
    }
    patch = {
        "spec": {
            "template": {
                "metadata": {
                    "annotations": {
                        "kubectl.kubernetes.io/default-container": "trino-mcp"
                    }
                },
                "spec": {
                    "containers": [sidecar],
                    "volumes": [
                        {
                            "name": "oil-runtime-gateway",
                            "configMap": {"name": "oil-runtime-gateway"},
                        },
                        {
                            "name": "oil-runtime-auth",
                            "secret": {"secretName": "oil-runtime-gateway-auth"},
                        },
                        {
                            "name": "oil-runtime-tls",
                            "secret": {"secretName": "oil-runtime-mcp-tls"},
                        },
                        {"name": "oil-runtime-tmp", "emptyDir": {}},
                    ],
                },
            }
        }
    }
    kubectl(
        [
            "patch",
            "deploy",
            "trino-mcp",
            "-n",
            NS,
            "--type=strategic",
            "--patch-file",
            "/dev/stdin",
        ],
        patch,
    )
    kubectl(
        [
            "patch",
            "svc",
            "trino-mcp",
            "-n",
            NS,
            "--type=strategic",
            "--patch-file",
            "/dev/stdin",
        ],
        {
            "spec": {
                "ports": [
                    {
                        "name": "runtime-tls",
                        "port": 8443,
                        "targetPort": 8443,
                        "protocol": "TCP",
                    }
                ]
            }
        },
    )
    # There was no previous ingress policy: preserve the legacy port's reachability
    # while restricting the new authenticated route to Airflow and this namespace.
    apply(
        {
            "apiVersion": "networking.k8s.io/v1",
            "kind": "NetworkPolicy",
            "metadata": {"name": "oil-runtime-mcp", "namespace": NS},
            "spec": {
                "podSelector": {
                    "matchLabels": {"app.kubernetes.io/name": "trino-mcp-cluster"}
                },
                "policyTypes": ["Ingress"],
                "ingress": [
                    {"ports": [{"port": 8000, "protocol": "TCP"}]},
                    {
                        "from": [
                            {
                                "namespaceSelector": {
                                    "matchLabels": {"kubernetes.io/metadata.name": AF}
                                }
                            },
                            {
                                "namespaceSelector": {
                                    "matchLabels": {"kubernetes.io/metadata.name": NS}
                                }
                            },
                        ],
                        "ports": [{"port": 8443, "protocol": "TCP"}],
                    },
                ],
            },
        }
    )
    evidence = {
        "mcp_url": "https://trino-mcp." + NS + ".svc.cluster.local:8443/mcp",
        "catalog_mcp_url": "https://trino-mcp."
        + NS
        + ".svc.cluster.local:8443/catalog-mcp",
        "lakekeeper_verify_url": "https://trino-mcp."
        + NS
        + ".svc.cluster.local:8443/verify-serving",
        "ca_file": "/etc/oil-wave2/mcp/ca.crt",
        "bearer_file": "/etc/oil-wave2/mcp/token",
        "airflow_secret": "oil-wave2-mcp-runtime",
        "transport": "TLS with bearer authentication",
        "underlying_identity": "existing Trino MCP service identity; not end-user impersonation",
    }
    (Path(__file__).parent / "runtime-tls-evidence.json").write_text(
        json.dumps(evidence, indent=2) + "\n"
    )
    print(json.dumps({"status": "configured", "mcp_url": evidence["mcp_url"]}))


if __name__ == "__main__":
    main()
