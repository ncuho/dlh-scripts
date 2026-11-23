#!/usr/bin/env python3
# Контекст миграции: AS IS / TO BE.
# Назначение: Проверяет действительность fence через локальный канал управляющей задачи; используется runner перед операциями.
# AS IS: Старый offline-маршрут удерживает source boundary и producer fence.
# TO BE: Сохранён для совместимости; текущая online-цепочка использует bridge_capture.
# Отличие: Этот адаптер не требуется запускать для online INITIAL/REFRESH.
# Конец контекста миграции.
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
