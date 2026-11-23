# Контекст миграции: AS IS / TO BE.
# Назначение: Проверяет структуру manifest, идентификаторы таблиц, snapshots и offset-диапазоны; атомарно сохраняет evidence.
# AS IS: Source checkpoint обозначает принятый/применённый диапазон событий.
# TO BE: Валидирует manifest, snapshots и полное ограниченное окно raw.
# Отличие: Один последний интервал не заменяет накопительную дельту от INITIAL C0.
# Конец контекста миграции.
"""Validate the bounded release manifest and persist evidence atomically."""

import json
import os
from pathlib import Path
import re

FQN = re.compile(r"^dlh\.[a-z][a-z0-9_]{0,62}\.[a-z][a-z0-9_]{0,62}$")
RELEASE = re.compile(r"^oil_r_[a-z0-9_]{1,42}$")


def manifest(path):
    """Проверяет и нормализует границу по всем партициям, не подменяя её broker end."""
    x = json.loads(Path(path).read_text())
    for name in ("baseline_table", "delta_table"):
        if not isinstance(x.get(name), str) or not FQN.fullmatch(x[name]):
            raise ValueError("invalid " + name)
    for name in ("baseline_snapshot", "delta_snapshot"):
        if type(x.get(name)) is not int or x[name] <= 0:
            raise ValueError("positive snapshot ID required: " + name)
    if not isinstance(x.get("cut_id"), str) or not x["cut_id"]:
        raise ValueError("cut_id required")
    for key in ("topic_id", "cluster_id"):
        if not isinstance(x.get(key), str) or not re.fullmatch(
            r"[A-Za-z0-9_-]{1,128}", x[key]
        ):
            raise ValueError("valid " + key + " required")
    offsets = x.get("partitions")
    if not isinstance(offsets, list) or not offsets:
        raise ValueError("nonempty Kafka partitions required")
    seen = set()
    for part in offsets:
        if (
            not isinstance(part, dict)
            or type(part.get("partition")) is not int
            or type(part.get("start")) is not int
            or type(part.get("end")) is not int
        ):
            raise ValueError("offset bounds must be integers")
        if (
            part["partition"] < 0
            or part["partition"] in seen
            or not 0 <= part["start"] <= part["end"]
        ):
            raise ValueError("invalid or duplicate partition")
        seen.add(part["partition"])
    return x


def write_json(path, obj):
    """Атомарно сохраняет JSON доказательство границы или результата выполнения."""
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(obj, ensure_ascii=False, indent=2) + "\n")
    os.replace(tmp, path)
