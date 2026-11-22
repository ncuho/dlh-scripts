#!/usr/bin/env python3
"""Bounded callback to the owning Airflow task's live source fence."""
import os, socket, sys

try:
    s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    s.settimeout(35)
    s.connect(os.environ["OIL_FENCE_SOCKET"])
    s.sendall(b"status\n")
    out = b""
    while len(out) < 65536:
        part = s.recv(4096)
        if not part:
            break
        out += part
        if b"\n" in out:
            break
    s.close()
    sys.stdout.buffer.write(out.split(b"\n", 1)[0] + b"\n")
except Exception:
    print('{"error":"fence_unavailable"}')
    raise SystemExit(1)
