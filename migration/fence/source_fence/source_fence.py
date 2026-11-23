# Удерживает живую границу на время build: lease Kafka, запрет producer write, pause GP sink и GP advisory lock. При потере подтверждения прекращает работу.
# Этот адаптер не требуется запускать для online INITIAL/REFRESH.
"""One-task Kafka lease + live Greenplum advisory lock for a bounded release.

The external command-only controller is responsible for fail-closed broker ACL,
producer deny and pausing/resuming the correct source sink. Credentials stay in
its managed SSH configuration. The lease ID is kept in process memory only.
"""

import hashlib, json, os, socketserver, subprocess, sys, tempfile, threading, time
from pathlib import Path


class SourceFence:
    def __init__(self, manifest, controller):
        """Сохраняет зависимости и настройки экземпляра; секреты остаются в штатных сервисах."""
        self.x = manifest
        self.controller = controller
        self.lease = None
        self.conn = None
        self.server = None
        self.thread = None
        self.directory = None
        if not Path(controller).is_absolute():
            raise ValueError("absolute controller path required")

    def call(self, action):
        """Выполняет один запрос к сервису или провайдеру с явно заданным контекстом и проверкой ответа."""
        body = {"action": action, "cut_id": self.x["cut_id"]}
        if self.lease:
            body["lease_id"] = self.lease
        args = (
            [sys.executable, self.controller]
            if self.controller.endswith(".py")
            else [self.controller]
        )
        p = subprocess.run(
            args, input=json.dumps(body), capture_output=True, text=True, timeout=45
        )
        if p.returncode:
            raise RuntimeError("Kafka fence controller failed")
        return json.loads(p.stdout)

    def __enter__(self):
        """Получает ресурсы контекстного менеджера; при ошибке освобождает уже полученные блокировки."""
        try:
            acquired = self.call("acquire")
            self.lease = acquired.get("lease_id")
            if (
                acquired.get("status") != "held"
                or acquired.get("cut_id") != self.x["cut_id"]
                or not self.lease
            ):
                raise RuntimeError("Kafka lease not held")
            # Existing Airflow connection is independently configured with verify-full.
            from airflow.hooks.base import BaseHook
            from airflow.providers.postgres.hooks.postgres import PostgresHook

            cfg = BaseHook.get_connection("webinar_greenplum")
            ca = Path(
                "/opt/airflow/dags/managed/gp-ca-cfb82a1e7313b52c6bef6d2a12b46cd55e5ade103340242ccf23e1295a92dc0b.crt"
            )
            if (
                hashlib.sha256(ca.read_bytes()).hexdigest()
                != "cfb82a1e7313b52c6bef6d2a12b46cd55e5ade103340242ccf23e1295a92dc0b"
            ):
                raise RuntimeError("GP CA digest mismatch")
            if (
                (cfg.host, cfg.schema, cfg.login, cfg.port)
                != ("160.5.219.123", "og_dwh", "demo_airflow_runner", 5432)
                or cfg.extra_dejson.get("sslmode") != "verify-full"
                or cfg.extra_dejson.get("sslrootcert") != str(ca)
            ):
                raise RuntimeError("unapproved GP connection")
            conn = PostgresHook(postgres_conn_id="webinar_greenplum").get_conn()
            conn.autocommit = True
            self.conn = conn
            with conn.cursor() as c:
                c.execute("SELECT pg_try_advisory_lock(%s)", (860223191,))
                if c.fetchone()[0] is not True:
                    raise RuntimeError("GP writer lock unavailable")
            self.directory = tempfile.TemporaryDirectory(prefix="oil-fence-")
            os.chmod(self.directory.name, 0o700)
            path = str(Path(self.directory.name) / "status.sock")
            owner = self

            class Handler(socketserver.StreamRequestHandler):
                def handle(self):
                    """Обрабатывает один запрос локального контрольного канала и проверяет действительность lease."""
                    try:
                        self.rfile.readline(32)
                        result = owner.status()
                    except Exception:
                        result = {"error": "fence_invalid"}
                    self.wfile.write((json.dumps(result) + "\n").encode())

            self.server = socketserver.UnixStreamServer(path, Handler)
            self.server.timeout = 1
            self.thread = threading.Thread(
                target=self.server.serve_forever, daemon=True
            )
            self.thread.start()
            self.socket_path = path
            # Capture broker identity and end offsets while both fences are held.
            if self.x.get("_capture"):
                observation = self.call("status")
                obs = observation.get("observation", {})
                ends = observation.get("next_offsets", {})
                if (
                    observation.get("status") != "held"
                    or observation.get("cut_id") != self.x["cut_id"]
                    or not isinstance(ends, dict)
                    or not ends
                ):
                    raise RuntimeError("cannot capture Kafka cut")
                old = self.x.get("_initial_partitions")
                starts = {str(p["partition"]): p["end"] for p in old} if old else ends
                if set(starts) != set(ends):
                    raise RuntimeError("partition set changed since baseline")
                self.x.update(
                    topic="oil.events.v1",
                    topic_id=obs.get("topic_id"),
                    cluster_id=obs.get("cluster_id"),
                    partitions=[
                        {"partition": int(k), "start": starts[k], "end": v}
                        for k, v in sorted(ends.items(), key=lambda item: int(item[0]))
                    ],
                )
                if old and (
                    self.x["topic_id"] != self.x.get("_initial_topic_id")
                    or self.x["cluster_id"] != self.x.get("_initial_cluster_id")
                ):
                    raise RuntimeError("Kafka identity changed since baseline")
            # First status verifies both independent fences before starting a release.
            self.status()
            print(json.dumps({"kind": "fence", "status": "held"}), flush=True)
            return self
        except BaseException:
            self.close()
            raise

    def status(self):
        """Проверяет фактическую доступность провайдера или удерживаемого ресурса."""
        if not self.lease or not self.conn or self.conn.closed:
            raise RuntimeError("no live source fence")
        with self.conn.cursor() as c:
            c.execute(
                "SELECT EXISTS (SELECT 1 FROM pg_locks WHERE locktype='advisory' AND pid=pg_backend_pid() AND granted AND classid=0 AND objid=860223191 AND objsubid=1)"
            )
            if c.fetchone()[0] is not True:
                raise RuntimeError("GP session lock lost")
        status = self.call("status")
        obs = status.get("observation", {})
        expected = {str(p["partition"]): p["end"] for p in self.x["partitions"]}
        if (
            status.get("status") != "held"
            or status.get("cut_id") != self.x["cut_id"]
            or status.get("next_offsets") != expected
        ):
            raise RuntimeError("Kafka fence lost or cut changed")
        if (
            obs.get("topic") != "oil.events.v1"
            or obs.get("topic_id") != self.x["topic_id"]
            or obs.get("cluster_id") != self.x["cluster_id"]
            or obs.get("producer_denied") is not True
        ):
            raise RuntimeError("Kafka identity or producer denial mismatch")
        if obs.get("known_oil_generator_processes") != []:
            raise RuntimeError("known generator is running or unobserved")
        connector = obs.get("connector", {})
        tasks = connector.get("tasks", [])
        if (
            connector.get("connector", {}).get("state") != "PAUSED"
            or not tasks
            or any(t.get("state") != "PAUSED" for t in tasks)
        ):
            raise RuntimeError("source sink not paused")
        low = {str(p["partition"]): p["low"] for p in obs.get("partitions", [])}
        if set(low) != set(expected) or any(
            low[str(p["partition"])] > p["start"] for p in self.x["partitions"]
        ):
            raise RuntimeError("Kafka retention gap")
        return {
            "producer_fenced": True,
            "gp_sink_paused": True,
            "gp_apply_fenced": True,
            "topic": self.x.get("topic", "oil.events.v1"),
            "topic_id": self.x["topic_id"],
            "cluster_id": self.x["cluster_id"],
            "cut_id": self.x["cut_id"],
            "valid_until_epoch": time.time() + 30,
            "next_offsets": expected,
        }

    def close(self):
        """Закрывает принадлежащую запуску сессию или ресурс; бизнес-данные не удаляет."""
        if self.server:
            self.server.shutdown()
            self.server.server_close()
            self.server = None
        if self.conn:
            try:
                self.conn.close()
            finally:
                self.conn = None
        if self.lease:
            try:
                result = self.call("release")
                if result.get("status") != "released":
                    raise RuntimeError("Kafka fence release not confirmed")
                print(json.dumps({"kind": "fence", "status": "released"}), flush=True)
            finally:
                self.lease = None
        if self.directory:
            self.directory.cleanup()
            self.directory = None

    def __exit__(self, typ, value, tb):
        """Освобождает ресурсы контекста, сохраняя исходную причину ошибки."""
        self.close()
        return False
