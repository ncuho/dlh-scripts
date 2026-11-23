#!/usr/bin/env python3
# Контекст миграции: AS IS / TO BE.
# Назначение: Адаптер ограниченного SSH-вызова acquire/status/release удалённого контроллера fence.
# AS IS: Старый offline-маршрут удерживает source boundary и producer fence.
# TO BE: Сохранён для совместимости; текущая online-цепочка использует bridge_capture.
# Отличие: Этот адаптер не требуется запускать для online INITIAL/REFRESH.
# Конец контекста миграции.
"""Fixed command-only SSH relay; lease JSON passes only through stdin/stdout."""
import os, subprocess, sys, tempfile
from pathlib import Path


def main():
    """Точка входа: Fixed command-only SSH relay; lease JSON passes only through stdin/stdout. Импорт модуля сам по себе эту операцию не запускает."""
    payload = sys.stdin.buffer.read(65537)
    if len(payload) > 65536:
        raise ValueError("controller input exceeds limit")
    secret = Path("/etc/oil-wave2/fence/id_ed25519")
    known = "/etc/oil-wave2/fence/known_hosts"
    with tempfile.TemporaryDirectory(prefix="oil-fence-key-", dir="/dev/shm") as d:
        key = Path(d) / "key"
        key.write_bytes(secret.read_bytes())
        os.chmod(key, 0o600)
        args = [
            "ssh",
            "-T",
            "-i",
            str(key),
            "-o",
            "IdentitiesOnly=yes",
            "-o",
            "BatchMode=yes",
            "-o",
            "StrictHostKeyChecking=yes",
            "-o",
            "UserKnownHostsFile=" + known,
            "-o",
            "ConnectTimeout=8",
            "-o",
            "ServerAliveInterval=10",
            "oil-fence-relay@10.0.2.7",
        ]
        r = subprocess.run(args, input=payload, capture_output=True, timeout=90)
        if r.returncode:
            raise RuntimeError("fence relay failed")
        sys.stdout.buffer.write(r.stdout)


if __name__ == "__main__":
    try:
        main()
    except Exception:
        print('{"error":"fence_relay_failed"}', file=sys.stderr)
        raise SystemExit(1)
