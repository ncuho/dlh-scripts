"""Bounded Cedrus release runner. A source fence and durable evidence are mandatory.

A successful `ready` is a reconciled immutable release, not BI publication.
MCP URL must be HTTPS except loopback; caller controls the separately secured fence probe.
"""

import argparse, hashlib, json, os, re, subprocess, sys, time, urllib.request, urllib.parse, ssl
from pathlib import Path
import sql_plan, dq_sql
from bounded_delta import manifest as read_manifest, write_json, FQN


class MCP:
    def __init__(self, url, ca, bearer_file):
        p = urllib.parse.urlparse(url)
        if p.scheme != "https" and not (
            p.scheme == "http" and p.hostname in ("127.0.0.1", "localhost", "::1")
        ):
            raise ValueError("MCP requires HTTPS or loopback")
        self.url = url
        self.session = None
        self.seq = 0
        self.ca = ca
        self.bearer_file = bearer_file
        self.rpc(
            "initialize",
            {
                "protocolVersion": "2024-11-05",
                "capabilities": {},
                "clientInfo": {"name": "oil-bounded-release", "version": "1"},
            },
            True,
        )
        self.rpc("notifications/initialized", {}, False)

    def rpc(self, method, params, answer=True):
        self.seq += 1
        body = {"jsonrpc": "2.0", "method": method, "params": params}
        if answer:
            body["id"] = self.seq
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json, text/event-stream",
        }
        if self.session:
            headers["Mcp-Session-Id"] = self.session
        if self.bearer_file:
            headers["Authorization"] = (
                "Bearer " + Path(self.bearer_file).read_text().strip()
            )
        req = urllib.request.Request(
            self.url, data=json.dumps(body).encode(), headers=headers
        )
        with urllib.request.urlopen(
            req, timeout=180, context=ssl.create_default_context(cafile=self.ca)
        ) as r:
            self.session = r.headers.get("Mcp-Session-Id", self.session)
            if not answer:
                return None
            if "text/event-stream" in r.headers.get("Content-Type", ""):
                result = None
                for line in r:
                    if line.startswith(b"data:"):
                        x = json.loads(line[5:])
                        if x.get("id") == self.seq:
                            result = x
                            break
                if result is None:
                    raise RuntimeError("MCP response missing")
            else:
                result = json.load(r)
        if result.get("error") or result.get("result", {}).get("isError"):
            raise RuntimeError("MCP tool error")
        return result.get("result", {})

    def sql(self, query):
        r = self.rpc(
            "tools/call", {"name": "execute_query", "arguments": {"query": query}}
        )
        value = r.get("structuredContent", {}).get("result")
        if value is None:
            raise RuntimeError("MCP result missing")
        try:
            return json.loads(value) if isinstance(value, str) else value
        except json.JSONDecodeError:
            return []


def verify_fence(command, x, mcp):
    if not command or not Path(command).is_absolute():
        raise ValueError("absolute fence-probe executable required")
    args = [sys.executable, command] if command.endswith(".py") else [command]
    p = subprocess.run(args, capture_output=True, text=True, timeout=45)
    if p.returncode:
        raise RuntimeError("fence probe failed")
    e = json.loads(p.stdout)
    if not all(
        e.get(k) is True
        for k in ("producer_fenced", "gp_sink_paused", "gp_apply_fenced")
    ):
        raise RuntimeError("fence not proven")
    if e.get("topic") != x.get("topic", "oil.events.v1"):
        raise RuntimeError("fence topic mismatch")
    if e.get("topic_id") != x["topic_id"] or e.get("cluster_id") != x["cluster_id"]:
        raise RuntimeError("Kafka identity mismatch")
    if (
        e.get("cut_id") != x["cut_id"]
        or e.get("valid_until_epoch", 0) <= time.time() + 10
    ):
        raise RuntimeError("fence expired or wrong cut")
    want = {str(p["partition"]): p["end"] for p in x["partitions"]}
    if e.get("next_offsets") != want:
        raise RuntimeError("broker cut differs")
    rows = mcp.sql(
        "SELECT kafka_partition,next_offset FROM greenplum_gp.oil_stg.kafka_checkpoint WHERE topic='oil.events.v1'"
    )
    actual = {str(r["kafka_partition"]): r["next_offset"] for r in rows}
    if actual != want:
        raise RuntimeError("GP checkpoints differ from broker")


def snap(mcp, table):
    catalog, schema, name = table.split(".")
    rows = mcp.sql(
        f'SELECT snapshot_id FROM {catalog}.{schema}."{name}$history" ORDER BY made_current_at DESC LIMIT 1'
    )
    if len(rows) != 1 or type(rows[0]["snapshot_id"]) is not int:
        raise RuntimeError("missing snapshot " + table)
    return rows[0]["snapshot_id"]


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--manifest", required=True)
    p.add_argument("--schema", required=True)
    p.add_argument("--evidence", required=True)
    p.add_argument("--fence-probe", required=True)
    p.add_argument("--resume", action="store_true")
    a = p.parse_args()
    x = read_manifest(a.manifest)
    if not sql_plan.SCHEMA.fullmatch(a.schema):
        raise ValueError("release namespace must be oil_r_*")
    mf = Path(a.manifest).read_bytes()
    fingerprint = hashlib.sha256(mf).hexdigest()
    e = Path(a.evidence)
    if a.resume:
        state = json.loads(e.read_text())
        if (
            state.get("manifest_sha256") != fingerprint
            or state.get("schema") != a.schema
            or state.get("status") == "ready"
        ):
            raise RuntimeError("resume evidence mismatch or already ready")
    else:
        if e.exists():
            raise FileExistsError("evidence exists; use audited resume")
        with e.open("x") as f:
            json.dump(
                {
                    "status": "initializing",
                    "manifest_sha256": fingerprint,
                    "schema": a.schema,
                },
                f,
            )
        state = {
            "status": "building",
            "manifest_sha256": fingerprint,
            "schema": a.schema,
            "cut_id": x["cut_id"],
            "input_snapshots": {
                "baseline": x["baseline_snapshot"],
                "delta": x["delta_snapshot"],
            },
            "partitions": x["partitions"],
            "steps": {},
            "checks": {},
            "snapshots": {},
        }
    write_json(e, state)
    try:
        m = MCP(
            os.environ.get("OIL_TRINO_MCP_URL", "http://127.0.0.1:8000/mcp"),
            os.environ.get("OIL_MCP_CA_FILE"),
            os.environ.get("OIL_MCP_BEARER_FILE"),
        )
        verify_fence(a.fence_probe, x, m)
        for prefix in ("baseline", "delta"):
            if snap(m, x[prefix + "_table"]) != x[prefix + "_snapshot"]:
                raise RuntimeError(prefix + " snapshot changed")
        target = "dlh." + a.schema
        windows = " OR ".join(
            "(kafka_partition="
            + str(v["partition"])
            + " AND kafka_offset>="
            + str(v["start"])
            + " AND kafka_offset<"
            + str(v["end"])
            + ")"
            for v in x["partitions"]
        )
        topic = x.get("topic", "oil.events.v1")
        if topic != "oil.events.v1":
            raise ValueError("unexpected topic")
        raw = f"{x['delta_table']} FOR VERSION AS OF {x['delta_snapshot']}"
        base = f"{x['baseline_table']} FOR VERSION AS OF {x['baseline_snapshot']}"
        for v in x["partitions"]:
            q = f"SELECT count(DISTINCT kafka_offset) AS n FROM {raw} WHERE topic='oil.events.v1' AND kafka_partition={v['partition']} AND kafka_offset>={v['start']} AND kafka_offset<{v['end']}"
            if m.sql(q)[0]["n"] != v["end"] - v["start"]:
                raise RuntimeError("offset gap " + str(v["partition"]))
        if (
            m.sql(
                f"SELECT count(*) AS n FROM (SELECT topic,kafka_partition,kafka_offset FROM {raw} WHERE topic='oil.events.v1' AND ({windows}) GROUP BY 1,2,3 HAVING count(DISTINCT event_json)>1)"
            )[0]["n"]
            != 0
        ):
            raise RuntimeError("Kafka position conflict")
        source = f"SELECT event_document AS document FROM {base} UNION ALL SELECT event_json AS document FROM {raw} WHERE topic='oil.events.v1' AND ({windows})"
        canonical = f"SELECT document,json_extract_scalar(document,'$.event_id') AS event_id,json_extract_scalar(document,'$.event_type') AS event_type,json_extract_scalar(document,'$.entity_id') AS entity_id,CAST(json_extract_scalar(document,'$.entity_version') AS BIGINT) AS entity_version,json_extract_scalar(document,'$.op') AS op,from_iso8601_timestamp(json_extract_scalar(document,'$.event_time')) AS event_time FROM (SELECT DISTINCT document FROM {target}.canonical_input)"
        commands = [
            (
                "canonical_input",
                f"CREATE TABLE {target}.canonical_input WITH (format='PARQUET') AS "
                + source,
            ),
            (
                "canonical_event",
                f"CREATE TABLE {target}.canonical_event WITH (format='PARQUET') AS "
                + canonical,
            ),
        ] + sql_plan.plan(a.schema)
        allowed = {name for name, _ in commands}
        for name, sql in commands:
            verify_fence(a.fence_probe, x, m)
            if (
                snap(m, x["baseline_table"]) != x["baseline_snapshot"]
                or snap(m, x["delta_table"]) != x["delta_snapshot"]
            ):
                raise RuntimeError("input snapshot changed")
            existing = {row["Table"] for row in m.sql("SHOW TABLES IN " + target)}
            if existing - allowed:
                raise RuntimeError("unexpected objects in release namespace")
            if name in existing:
                if not a.resume or name not in state["steps"]:
                    raise RuntimeError("unexpected existing target " + name)
                # A timed-out CTAS may still be running; explicit operator attestation is required.
                if (
                    state["steps"][name] == "attempting"
                    and os.environ.get("OIL_ABORTED_QUERY_CONFIRMED") != "yes"
                ):
                    raise RuntimeError(
                        "unknown CTAS outcome: reconcile execution before resume"
                    )
                select = sql.split(" AS ", 1)[1]
                n = m.sql(
                    f"SELECT count(*) AS n FROM ((SELECT * FROM {target}.{name} EXCEPT ALL ({select})) UNION ALL (({select}) EXCEPT ALL SELECT * FROM {target}.{name}))"
                )[0]["n"]
                if n:
                    raise RuntimeError("existing release table mismatch " + name)
            else:
                if (
                    a.resume
                    and name in state["steps"]
                    and state["steps"][name] != "attempting"
                ):
                    raise RuntimeError("previously completed table missing")
                state["steps"][name] = "attempting"
                write_json(e, state)
                print(
                    json.dumps({"kind": "step", "name": name, "status": "attempting"}),
                    flush=True,
                )
                m.sql(sql)
            state["steps"][name] = "created"
            state["snapshots"][name] = snap(m, target + "." + name)
            write_json(e, state)
            print(
                json.dumps(
                    {
                        "kind": "step",
                        "name": name,
                        "status": "verified",
                        "snapshot_id": state["snapshots"][name],
                    }
                ),
                flush=True,
            )
        for name, q in dq_sql.gates(a.schema) + dq_sql.comparisons(a.schema):
            verify_fence(a.fence_probe, x, m)
            n = m.sql(q)[0]["n"]
            state["checks"][name] = n
            write_json(e, state)
            print(json.dumps({"kind": "dq", "name": name, "violations": n}), flush=True)
            if n != 0:
                raise RuntimeError("DQ/reconciliation failed " + name)
        # Detect changed target snapshots after all comparisons, before publishing eligibility.
        for name, sid in state["snapshots"].items():
            if snap(m, target + "." + name) != sid:
                raise RuntimeError("target snapshot changed " + name)
        verify_fence(a.fence_probe, x, m)
        state["status"] = "ready"
        write_json(e, state)
        print(
            json.dumps({"kind": "result", "status": "ready", "schema": a.schema}),
            flush=True,
        )
        return 0
    except Exception as exc:
        state["status"] = "failed_reconcile_before_retry"
        state["error_type"] = type(exc).__name__
        write_json(e, state)
        print(
            json.dumps({"status": "failed", "error_type": type(exc).__name__}),
            file=sys.stderr,
        )
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
