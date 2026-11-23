"""Stage categorized sources into the flat managed Airflow module directory."""

import argparse
from pathlib import Path
import shutil

ROOT = Path(__file__).resolve().parents[2]
MODULE_DIRS = ("pipelines", "migration/fence", "publishing")


def main():
    """Точка входа: Stage categorized sources into the flat managed Airflow module directory. Импорт модуля сам по себе эту операцию не запускает."""
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    target = Path(args.output).expanduser().resolve()
    if target.exists() and any(target.iterdir()):
        raise SystemExit("output must be empty; do not overwrite a running deployment")
    target.mkdir(parents=True, exist_ok=True)
    for folder in MODULE_DIRS:
        for source in (ROOT / folder).rglob("*.py"):
            shutil.copy2(source, target / source.name)
    shutil.copy2(
        ROOT / "catalog/runtime_catalog/runtime_catalog.py",
        target / "runtime_catalog.py",
    )
    shutil.copy2(
        ROOT / "dags/oil_wave2_release/oil_wave2_release.py",
        target / "airflow_oil_wave2.py",
    )
    print("Runtime package staged; no service was changed")


if __name__ == "__main__":
    main()
