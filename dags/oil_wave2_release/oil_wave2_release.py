"""Manual oil release orchestration. Deployment/configuration required.

The build holds Kafka controller lease and Greenplum advisory lock in this one
Airflow task; a crashed lease is fail-closed and requires operator recovery.
Publishing is a separate, explicit trigger and requires table-level grant evidence.
"""

from datetime import datetime
from pathlib import Path
import fcntl
import json
import os
import re
import subprocess
import sys
import threading
import time
from airflow import DAG
from airflow.operators.python import PythonOperator

ROOT = Path("/opt/airflow/dags/managed/oil_wave2")
sys.path.insert(0, str(ROOT))
from bounded_delta import manifest as read_manifest
from source_fence import SourceFence
from bounded_delta import write_json
from cedrus_release_runner import MCP, snap
from superset_auth import publisher_token
import uuid
import hashlib
import ssl
import urllib.request

NAME = re.compile(r"^[a-z][a-z0-9_]{0,62}$")


def execute(**context):
    conf = context["dag_run"].conf or {}
    action = conf.get("action")
    schema = conf.get("schema", "")
    if action not in (
        "build",
        "publish",
        "catalog_ready",
        "catalog_published",
    ) or not re.fullmatch(r"oil_r_[a-z0-9_]{1,42}", schema):
        raise ValueError("valid action and isolated release schema required")
    base = Path(
        os.environ.get("OIL_RELEASE_EVIDENCE_DIR", "/opt/airflow/logs/oil_releases")
    )
    if not base.is_absolute():
        raise ValueError("evidence directory must be absolute")
    base.mkdir(parents=True, exist_ok=True, mode=0o700)
    lock = (base / ".oil_release.lock").open("a+")
    try:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        evidence = base / (schema + ".evidence.json")
        if action == "build":
            resume = conf.get("resume") is True
            manifest_path = base / (schema + ".manifest.json")
            mode = conf.get("mode")
            if resume:
                x = read_manifest(manifest_path)
                mode = x.get("mode")
                if not evidence.exists():
                    raise ValueError("no build evidence to resume")
            elif manifest_path.exists() or evidence.exists():
                raise FileExistsError(
                    "release already exists; inspect evidence before any retry"
                )
            if mode not in ("initial", "refresh"):
                raise ValueError("mode must be initial or refresh")
            initial = None
            if mode == "refresh" and not resume:
                initial_schema = conf.get("initial_schema", "")
                if not re.fullmatch(r"oil_r_[a-z0-9_]{1,42}", initial_schema):
                    raise ValueError("initial_schema required")
                initial_path = base / (initial_schema + ".manifest.json")
                initial = read_manifest(initial_path)
                initial_evidence = json.loads(
                    (base / (initial_schema + ".evidence.json")).read_text()
                )
                if (
                    initial_evidence.get("status") != "ready"
                    or initial.get("mode") != "initial"
                    or initial_evidence.get("manifest_sha256")
                    != hashlib.sha256(initial_path.read_bytes()).hexdigest()
                ):
                    raise ValueError("initial baseline is not verified ready")
            if not resume:
                x = {"cut_id": uuid.uuid4().hex, "_capture": True}
            if initial:
                x.update(
                    _initial_partitions=initial["partitions"],
                    _initial_topic_id=initial["topic_id"],
                    _initial_cluster_id=initial["cluster_id"],
                )
            controller = os.environ["OIL_FENCE_CONTROLLER"]
            env = os.environ.copy()
            with SourceFence(x, controller) as fence:
                env["OIL_FENCE_SOCKET"] = fence.socket_path
                m = MCP(
                    os.environ["OIL_TRINO_MCP_URL"],
                    os.environ.get("OIL_MCP_CA_FILE"),
                    os.environ.get("OIL_MCP_BEARER_FILE"),
                )
                rows = m.sql(
                    "SELECT kafka_partition,next_offset FROM greenplum_gp.oil_stg.kafka_checkpoint WHERE topic='oil.events.v1'"
                )
                if {str(r["kafka_partition"]): r["next_offset"] for r in rows} != {
                    str(p["partition"]): p["end"] for p in x["partitions"]
                }:
                    raise RuntimeError("GP checkpoints do not match fenced broker cut")
                fence.status()
                # A committed Kafka checkpoint only proves landing in STG, not application
                # to DDS. Never advance the baseline offset beyond an unapplied event.
                pending = m.sql(
                    "SELECT count(*) AS n FROM greenplum_gp.oil_stg.batch_control WHERE status IN ('OPEN','READY','FAILED')"
                )[0]["n"]
                unapplied = m.sql(
                    "SELECT count(*) AS n FROM greenplum_gp.oil_stg.event_log e LEFT JOIN greenplum_gp.oil_stg.batch_control b ON b.batch_id=e.batch_id WHERE e.topic='oil.events.v1' AND (b.batch_id IS NULL OR b.status<>'APPLIED')"
                )[0]["n"]
                missing = m.sql(
                    "SELECT count(*) AS n FROM (SELECT DISTINCT e.event_json FROM greenplum_gp.oil_stg.event_log e WHERE e.topic='oil.events.v1' AND NOT EXISTS (SELECT 1 FROM greenplum_gp.oil_dds.event d WHERE d.event_document=e.event_json))"
                )[0]["n"]
                print(
                    json.dumps(
                        {
                            "kind": "source_gate",
                            "pending_batches": pending,
                            "unapplied_rows": unapplied,
                            "missing_events": missing,
                        }
                    ),
                    flush=True,
                )
                if pending or unapplied or missing:
                    raise RuntimeError(
                        "source STG to DDS is not fully applied; no cut created"
                    )
                fence.status()
                if not resume:
                    if mode == "initial":
                        input_schema = "oil_i_" + schema[6:]
                        baseline = "dlh." + input_schema + ".baseline_event"
                        m.sql("CREATE SCHEMA dlh." + input_schema)
                        m.sql(
                            "CREATE TABLE "
                            + baseline
                            + " WITH (format='PARQUET') AS SELECT * FROM greenplum_gp.oil_dds.event"
                        )
                        baseline_snapshot = snap(m, baseline)
                    else:
                        baseline = initial["baseline_table"]
                        baseline_snapshot = initial["baseline_snapshot"]
                        if snap(m, baseline) != baseline_snapshot:
                            raise RuntimeError("initial baseline changed")
                    fence.status()
                    delta = "dlh.oil_stg.event_log_wave2"
                    delta_snapshot = snap(m, delta)
                    x = {k: v for k, v in x.items() if not k.startswith("_")}
                    x.update(
                        mode=mode,
                        baseline_table=baseline,
                        baseline_snapshot=baseline_snapshot,
                        delta_table=delta,
                        delta_snapshot=delta_snapshot,
                    )
                    # Exclusive creation makes a partial or ambiguous preparation visible and non-retryable.
                    with manifest_path.open("x", encoding="utf8") as f:
                        json.dump(x, f, ensure_ascii=False, indent=2)
                    x = read_manifest(manifest_path)
                    m.sql("CREATE SCHEMA dlh." + schema)
                env["OIL_CUT_MANIFEST"] = str(manifest_path)
                if (
                    conf.get("resume") is True
                    and conf.get("confirm_previous_query_terminal") is True
                ):
                    env["OIL_ABORTED_QUERY_CONFIRMED"] = "yes"
                command = [
                    sys.executable,
                    str(ROOT / "cedrus_release_runner.py"),
                    "--manifest",
                    str(manifest_path),
                    "--schema",
                    schema,
                    "--evidence",
                    str(evidence),
                    "--fence-probe",
                    str(ROOT / "fence_probe_socket.py"),
                ]
                if conf.get("resume") is True:
                    command.append("--resume")
                env["PYTHONUNBUFFERED"] = "1"
                started = time.monotonic()
                proc = subprocess.Popen(
                    command,
                    env=env,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT,
                    text=True,
                    bufsize=1,
                )
                timer = threading.Timer(7200, proc.kill)
                timer.start()
                try:
                    for line in proc.stdout:
                        try:
                            record = json.loads(line)
                        except (ValueError, TypeError):
                            continue
                        if record.get("kind") in ("step", "dq", "result"):
                            safe = {
                                k: record[k]
                                for k in (
                                    "kind",
                                    "name",
                                    "status",
                                    "snapshot_id",
                                    "violations",
                                    "schema",
                                )
                                if k in record
                            }
                            print(json.dumps(safe, ensure_ascii=False), flush=True)
                    code = proc.wait()
                finally:
                    timer.cancel()
                print(
                    json.dumps(
                        {
                            "kind": "elapsed",
                            "seconds": round(time.monotonic() - started, 1),
                        }
                    ),
                    flush=True,
                )
                if code:
                    raise RuntimeError(
                        "release failed; inspect protected evidence; no blind retry"
                    )
                print("BUILD_RECONCILED", schema)
            catalog_cmd = [
                sys.executable,
                str(ROOT / "runtime_catalog.py"),
                "--evidence",
                str(evidence),
                "--manifest",
                str(manifest_path),
                "--phase",
                "ready",
            ]
            if subprocess.run(catalog_cmd, timeout=900).returncode:
                raise RuntimeError(
                    "release ready but catalog registration failed; retry catalog_ready"
                )
            print("CATALOG_READY", schema)
        elif action == "publish":
            dataset = conf.get("dataset_id")
            db = conf.get("database_id")
            expected_schema = conf.get("expected_schema", "")
            expected_table = conf.get("expected_table", "")
            table = conf.get("table", "")
            allowed = {
                int(v)
                for v in os.environ.get("OIL_ALLOWED_DATASET_IDS", "").split(",")
                if v.isdigit()
            }
            if (
                type(dataset) is not int
                or dataset not in allowed
                or type(db) is not int
                or not NAME.fullmatch(expected_schema)
                or not NAME.fullmatch(expected_table)
                or table not in ("agg_field_day", "field_performance")
            ):
                raise ValueError("publish target is not explicitly allowed")
            grant = base / (schema + ".grants.json")
            verify_url = os.environ["OIL_LAKEKEEPER_VERIFY_URL"]
            if (
                verify_url
                != "https://trino-mcp.trinomcpcluster-xmrnjrpc.svc.cluster.local:8443/verify-serving"
            ):
                raise ValueError("unapproved grant verifier endpoint")
            body = evidence.read_bytes()
            req = urllib.request.Request(
                verify_url,
                data=body,
                method="POST",
                headers={
                    "Content-Type": "application/json",
                    "Authorization": "Bearer "
                    + Path(os.environ["OIL_MCP_BEARER_FILE"]).read_text().strip(),
                },
            )
            tls = ssl.create_default_context(cafile=os.environ["OIL_MCP_CA_FILE"])
            with urllib.request.urlopen(req, context=tls, timeout=60) as response:
                verified = json.load(response)
            if (
                verified.get("schema") != schema
                or verified.get("serving_select_verified") is not True
            ):
                raise RuntimeError("current LakeKeeper grants not verified")
            write_json(grant, verified)
            command = [
                sys.executable,
                str(ROOT / "publish_dataset.py"),
                "--evidence",
                str(evidence),
                "--grant-evidence",
                str(grant),
                "--dataset-id",
                str(dataset),
                "--database-id",
                str(db),
                "--expected-schema",
                expected_schema,
                "--expected-table",
                expected_table,
                "--table",
                table,
                "--apply",
            ]
            with publisher_token() as token_path:
                pub_env = os.environ.copy()
                pub_env["OIL_SUPERSET_BEARER_FILE"] = token_path
                result = subprocess.run(
                    command, capture_output=True, text=True, timeout=180, env=pub_env
                )
            if result.returncode:
                raise RuntimeError(
                    "publication failed; inspect dataset binding and publication logs"
                )
            publication = json.loads(result.stdout)
            if (
                publication.get("status") != "switched"
                or publication.get("dataset_id") != dataset
                or publication.get("new") != {"schema": schema, "table_name": table}
            ):
                raise RuntimeError("publication readback not verified")
            write_json(base / (schema + ".publication.json"), publication)
            print("PUBLISH_VERIFIED", schema, dataset)
            with publisher_token() as token_path:
                cat_env = os.environ.copy()
                cat_env["OIL_SUPERSET_BEARER_FILE"] = token_path
                command = [
                    sys.executable,
                    str(ROOT / "runtime_catalog.py"),
                    "--evidence",
                    str(evidence),
                    "--manifest",
                    str(base / (schema + ".manifest.json")),
                    "--phase",
                    "published",
                    "--dataset-id",
                    str(dataset),
                    "--publication",
                    str(base / (schema + ".publication.json")),
                ]
                if subprocess.run(command, timeout=900, env=cat_env).returncode:
                    raise RuntimeError(
                        "publication succeeded but lineage registration failed; retry catalog_published"
                    )
            print("CATALOG_PUBLISHED", schema, dataset)
        else:
            phase = "ready" if action == "catalog_ready" else "published"
            command = [
                sys.executable,
                str(ROOT / "runtime_catalog.py"),
                "--evidence",
                str(evidence),
                "--manifest",
                str(base / (schema + ".manifest.json")),
                "--phase",
                phase,
            ]
            if phase == "published":
                command += [
                    "--dataset-id",
                    "8",
                    "--publication",
                    str(base / (schema + ".publication.json")),
                ]
                with publisher_token() as token_path:
                    cat_env = os.environ.copy()
                    cat_env["OIL_SUPERSET_BEARER_FILE"] = token_path
                    result = subprocess.run(command, timeout=900, env=cat_env)
            else:
                result = subprocess.run(command, timeout=900)
            if result.returncode:
                raise RuntimeError(
                    "catalog operation failed; inspect logs before retry"
                )
            print("CATALOG_UPDATED", schema, phase)
    finally:
        lock.close()


with DAG(
    dag_id="oil_wave2_release",
    description="Подготовка, сверка и публикация отчётности нефтедобычи в Lakehouse",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["oil", "manual", "bounded"],
) as dag:
    PythonOperator(task_id="execute_guarded_release", python_callable=execute)
