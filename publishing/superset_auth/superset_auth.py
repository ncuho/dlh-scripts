"""Short-lived publisher login over verified TLS; tokens live only in runtime tmpfs."""

from contextlib import contextmanager
import json
import os
from pathlib import Path
import ssl
import tempfile
import urllib.parse
import urllib.request


@contextmanager
def publisher_token():
    url = os.environ["OIL_SUPERSET_URL"].rstrip("/")
    parsed = urllib.parse.urlparse(url)
    if (
        parsed.scheme != "https"
        or parsed.hostname
        not in ("10.0.2.24", "superset.supersetcluster-6wlhwvb5.svc.cluster.local")
        or parsed.port not in (None, 443)
        or parsed.path
        or parsed.username
        or parsed.password
    ):
        raise ValueError("unapproved Superset publisher endpoint")
    username = (
        Path(
            os.environ.get(
                "OIL_SUPERSET_USERNAME_FILE", "/etc/oil-wave2/superset/username"
            )
        )
        .read_text()
        .strip()
    )
    password = (
        Path(
            os.environ.get(
                "OIL_SUPERSET_PASSWORD_FILE", "/etc/oil-wave2/superset/password"
            )
        )
        .read_text()
        .strip()
    )
    if username != "oil_migration_publisher" or not password:
        raise ValueError("dedicated publisher credentials required")
    ca = os.environ.get("OIL_SUPERSET_CA_FILE", "/etc/oil-wave2/mcp/ca.crt")
    ctx = ssl.create_default_context(cafile=ca)
    opener = urllib.request.build_opener(
        urllib.request.ProxyHandler({}), urllib.request.HTTPSHandler(context=ctx)
    )
    body = json.dumps(
        {"username": username, "password": password, "provider": "db", "refresh": False}
    ).encode()
    req = urllib.request.Request(
        url + "/api/v1/security/login",
        data=body,
        headers={"Content-Type": "application/json", "Accept": "application/json"},
    )
    with opener.open(req, timeout=25) as response:
        token = json.load(response)["access_token"]
    if not isinstance(token, str) or not token:
        raise RuntimeError("publisher token missing")
    folder = Path(os.environ.get("OIL_SUPERSET_TOKEN_DIR", "/run/oil-publisher"))
    if not folder.is_absolute():
        raise ValueError("absolute runtime token directory required")
    folder.mkdir(parents=True, exist_ok=True, mode=0o700)
    with tempfile.NamedTemporaryFile(
        mode="w", encoding="utf-8", prefix="publisher-", dir=folder, delete=False
    ) as f:
        path = Path(f.name)
        os.fchmod(f.fileno(), 0o600)
        f.write(token)
    try:
        yield str(path)
    finally:
        path.unlink(missing_ok=True)


if __name__ == "__main__":
    with publisher_token() as path:
        print(
            json.dumps(
                {
                    "status": "authenticated",
                    "token_permissions": oct(Path(path).stat().st_mode & 0o777),
                }
            )
        )
