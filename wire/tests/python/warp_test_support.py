"""Shared test harness: a real disposable Postgres container plus a real Warp subprocess,
driven via plain `docker` CLI and `java -jar` -- no mocks, matching this project's own
live-verification style. Used by test_orawire.py, test_mssqlwire.py, test_mywire.py.
"""
import http.client
import os
import socket
import subprocess
import threading
import time
import uuid

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
# WARP_TEST_JAR lets a test run use a jar built elsewhere (e.g. while another Warp still runs target/).
JAR_PATH = os.environ.get("WARP_TEST_JAR") or os.path.join(REPO_ROOT, "target", "sayonora-wire.jar")

ADD_OPENS = [
    "--add-opens=java.base/jdk.internal.access=ALL-UNNAMED",
    "--add-opens=java.base/jdk.internal.misc=ALL-UNNAMED",
    "--add-opens=java.base/jdk.internal.ref=ALL-UNNAMED",
    "--add-opens=java.base/java.lang=ALL-UNNAMED",
    "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED",
    "--add-opens=java.base/java.io=ALL-UNNAMED",
    "--add-opens=java.base/java.nio=ALL-UNNAMED",
    "--add-opens=java.base/java.util=ALL-UNNAMED",
    "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED",
    "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED",
    "--add-opens=java.base/java.util.concurrent.locks=ALL-UNNAMED",
    "--add-opens=java.base/java.math=ALL-UNNAMED",
    "--add-opens=java.base/java.time=ALL-UNNAMED",
    "--add-opens=java.base/java.text=ALL-UNNAMED",
    "--add-opens=java.base/java.net=ALL-UNNAMED",
    "--add-opens=java.sql/java.sql=ALL-UNNAMED",
    "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
]


def free_port():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("", 0))
        return s.getsockname()[1]


def docker_run_on_free_port(name, port_args_builder, attempts=5):
    """`docker run` with a freshly picked host port, retrying with a new port when docker cannot bind
    it (another process can grab an ephemeral port between free_port() and `docker run`).
    `port_args_builder(port)` returns the docker-run argument list AFTER `docker run -d --name <name>`.
    Returns the port that worked."""
    last = None
    for _ in range(attempts):
        port = free_port()
        result = subprocess.run(["docker", "run", "-d", "--name", name, *port_args_builder(port)],
                                capture_output=True, text=True)
        if result.returncode == 0:
            return port
        last = result
        subprocess.run(["docker", "rm", "-f", "-v", name], capture_output=True, text=True)
    raise subprocess.CalledProcessError(last.returncode, "docker run", last.stdout, last.stderr)


ALL_LISTEN_PORT_VARS = [
    "WARP_A2A_PORT", "WARP_BOLTWIRE_PORT", "WARP_DYNAMOWIRE_PORT", "WARP_INFLUXWIRE_PORT", "WARP_MCP_PORT",
    "WARP_MONGOWIRE_PORT", "WARP_OSWIRE_PORT", "WARP_S3WIRE_PORT", "WARP_SQSWIRE_PORT", "WARP_PGWIRE_PORT",
    "WARP_MYWIRE_PORT", "WARP_ORAWIRE_PORT", "WARP_MSSQLWIRE_PORT"]


def isolated_ports(exclude=None):
    """A free port for every Warp listener except `exclude` (the frontend WarpProcess already assigns),
    so two Warp processes -- or one alongside a dev Warp on the default ports -- never collide. Pass
    the result as (part of) `extra_env`."""
    return {name: str(free_port()) for name in ALL_LISTEN_PORT_VARS if name != exclude}


class LocalPostgres:
    """A real, disposable Postgres SERVER PROCESS (Homebrew `initdb`/`postgres`, scram password auth like the
    container) in a temp dir -- for machines where the Docker VM is out of disk. Opt-in via
    WARP_TEST_PG_LOCAL=1; RealPostgres delegates to it."""

    def __init__(self):
        import shutil
        import tempfile
        self.dir = tempfile.mkdtemp(prefix="warp-localpg-")
        self.port = free_port()
        datadir = os.path.join(self.dir, "data")
        pwfile = os.path.join(self.dir, "pw")
        with open(pwfile, "w") as f:
            f.write("postgres\n")
        bins = os.environ.get("WARP_TEST_PG_BIN", "")
        initdb = os.path.join(bins, "initdb") if bins else shutil.which("initdb")
        postgres = os.path.join(bins, "postgres") if bins else shutil.which("postgres")
        subprocess.run([initdb, "-D", datadir, "-U", "postgres", "--pwfile", pwfile, "-A", "scram-sha-256",
                        "-E", "UTF8", "--locale=C"], check=True, capture_output=True)
        self.process = subprocess.Popen(
            [postgres, "-D", datadir, "-p", str(self.port), "-k", self.dir, "-c", "listen_addresses=127.0.0.1",
             "-c", "max_connections=200", "-c", "max_prepared_transactions=10"],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, env={**os.environ, "LC_ALL": "en_US.UTF-8"})
        deadline = time.time() + 30
        while time.time() < deadline:
            try:
                with socket.create_connection(("127.0.0.1", self.port), timeout=1):
                    break
            except OSError:
                time.sleep(0.2)
        else:
            self.close()
            raise TimeoutError("local Postgres did not start")
        # the server accepts TCP slightly before it finished recovery; wait for a real login
        import psycopg2
        while time.time() < deadline:
            try:
                psycopg2.connect(host="127.0.0.1", port=self.port, user="postgres", password="postgres",
                                 dbname="postgres").close()
                return
            except Exception:  # noqa: BLE001
                time.sleep(0.2)
        self.close()
        raise TimeoutError("local Postgres did not accept logins")

    def close(self):
        import shutil
        if self.process.poll() is None:
            import signal
            # SIGINT = "fast shutdown" (disconnects clients); SIGTERM's smart shutdown waits for every client to
            # leave, which leaks the postmaster (and its SysV shared memory) when a Warp is still connected.
            self.process.send_signal(signal.SIGINT)
            try:
                self.process.wait(timeout=15)
            except subprocess.TimeoutExpired:
                self.process.kill()
        shutil.rmtree(self.dir, ignore_errors=True)


class RealPostgres:
    """A real, disposable Postgres container -- plain `docker run`, not a test-library
    abstraction, so it needs nothing beyond Docker itself being installed."""

    def __init__(self):
        if os.environ.get("WARP_TEST_PG_LOCAL") == "1":
            self._local = LocalPostgres()
            self.name = "local-" + os.path.basename(self._local.dir)
            self.port = self._local.port
            return
        self._local = None
        self.name = f"warp-pytest-pg-{uuid.uuid4().hex[:12]}"
        self.port = docker_run_on_free_port(self.name, lambda port: [
            "-p", f"{port}:5432",
            "-e", "POSTGRES_USER=postgres",
            "-e", "POSTGRES_PASSWORD=postgres",
            "-e", "POSTGRES_DB=postgres",
            "postgres:16-alpine",
        ])
        self._wait_ready()

    def _wait_ready(self, timeout=30):
        deadline = time.time() + timeout
        while time.time() < deadline:
            result = subprocess.run(
                ["docker", "exec", self.name, "pg_isready", "-U", "postgres"],
                capture_output=True, text=True,
            )
            if result.returncode == 0:
                return
            time.sleep(0.5)
        raise TimeoutError(f"Postgres container {self.name} did not become ready in {timeout}s")

    def close(self):
        if self._local is not None:
            self._local.close()
            return
        subprocess.run(["docker", "rm", "-f", "-v", self.name], capture_output=True, text=True)


class WarpProcess:
    """A real Warp process (the shaded jar), pointed at a real Postgres backend."""

    def __init__(self, postgres: RealPostgres, frontend_env_var: str, frontend_name="frontend",
                 extra_env=None):
        if not os.path.exists(JAR_PATH):
            raise RuntimeError(
                f"{JAR_PATH} not found -- run `mvn -DskipTests package` in wire/ before these tests")

        self.frontend_port = free_port()
        self.metrics_port = free_port()
        # Real gap found on this dev machine: Main/ServerOptions always stands up the native gRPC
        # QueryService too (ServerOptions.java parses WARP_GRPC_PORT, default 7070), even for tests
        # that only care about one wire frontend. A long-running, unrelated process on this box
        # holds 7070, so every WarpProcess startup raced "Failed to bind to address 0.0.0.0:7070"
        # -- confirmed live, not hypothetical. Always give gRPC its own free ephemeral port so no
        # test using this harness (old or new) can collide with whatever else is on the machine.
        self.grpc_port = free_port()
        self._frontend_name = frontend_name

        env = dict(os.environ)
        env.update({
            # Real bug, found live while chasing a Developer-license "instance cap" failure during
            # a full-suite pytest run (`liveElsewhere + 1 > max`, capped at 3): ServerOptions.java
            # actually reads WARP_HOST/WARP_PORT/WARP_DATABASE/WARP_USER/WARP_PASSWORD for its
            # config-primary/backend Postgres connection (see README.md's own env var table and
            # ServerOptions.parse) -- WARP_PG_HOST/WARP_PG_PORT/etc, the names previously set here,
            # are never read anywhere in the Java source (confirmed by grepping
            # src/main/java/com/sayonora/wire for them -- zero hits). Every WarpProcess launched by
            # this harness was therefore silently ignoring its own disposable RealPostgres
            # container and falling back to ServerOptions' own default (localhost:5432, i.e.
            # whatever real Postgres happens to be listening on the dev machine's default port) --
            # confirmed live: a stray HikariDataSource log line read
            # "jdbc:postgresql://localhost:5432/postgres" for a WarpProcess whose RealPostgres
            # container was actually bound to a different, randomly-chosen port. Every test in this
            # suite (old and new) was consequently sharing ONE uncontrolled external Postgres
            # instance instead of getting real per-test isolation, and their warp_nodes heartbeat
            # rows piled up in it across test files until the Developer license's instance cap
            # tripped. Fixed by using the real env var names.
            "WARP_HOST": "localhost",
            "WARP_PORT": str(postgres.port),
            "WARP_DATABASE": "postgres",
            "WARP_USER": "postgres",
            "WARP_PASSWORD": "postgres",
            "WARP_AUTH_USER": "postgres",
            "WARP_AUTH_PASSWORD": "postgres",
            "WARP_METRICS_PORT": str(self.metrics_port),
            "WARP_GRPC_PORT": str(self.grpc_port),
            # Default QoS admission control (rate=5/s burst=5, maxWaitMs=0 -- no queueing) is tuned
            # for production traffic shaping, not a test client's rapid connection-setup handshake;
            # without this a driver's own setup queries alone can trip "rate limit exceeded".
            "WARP_QOS_RATE_PER_SEC": "1000",
            "WARP_QOS_BURST": "1000",
            frontend_env_var: str(self.frontend_port),
        })
        # Additive hook for callers that need extra env vars on the Warp process (e.g. cache /
        # cluster config) without every existing caller having to know about them.
        if extra_env:
            env.update({k: str(v) for k, v in extra_env.items()})

        java_bin = os.path.join(os.environ.get("JAVA_HOME", ""), "bin", "java") if os.environ.get("JAVA_HOME") else "java"
        cmd = [java_bin, *ADD_OPENS, "-jar", JAR_PATH]
        self.process = subprocess.Popen(
            cmd, env=env, cwd=REPO_ROOT,
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
        )
        # Ignite/Jetty startup alone logs enough that an undrained pipe fills its OS buffer and
        # blocks the JVM's write -- the process then silently stalls mid-startup (a listener
        # thread that hadn't started yet never does), not a clean crash. Found live: every
        # mssqlwire test failed with "connection refused" even though /metrics (which happens to
        # start earlier) was already up. Drain continuously on a daemon thread instead of only in
        # close(), matching WarpProcess.java's own working pattern.
        self._output_lines = []
        self._drain_thread = threading.Thread(target=self._drain_output, daemon=True)
        self._drain_thread.start()
        try:
            self._wait_ready()
        except BaseException:
            # never leave a half-started JVM (it would hold ports and a license instance slot)
            self.close()
            raise

    def _drain_output(self):
        try:
            for line in self.process.stdout:
                self._output_lines.append(line)
        except Exception:  # noqa: BLE001 -- process pipe closing during teardown is expected
            pass

    def _wait_ready(self, timeout=30):
        # Checks both /metrics AND the actual frontend port -- /metrics starts early in Main's
        # setup, before every protocol listener thread has necessarily started, so it alone isn't
        # sufficient proof the frontend under test is actually accepting connections yet.
        deadline = time.time() + timeout
        last_error = None
        while time.time() < deadline:
            try:
                conn = http.client.HTTPConnection("localhost", self.metrics_port, timeout=1)
                conn.request("GET", "/metrics")
                resp = conn.getresponse()
                resp.read()
                if resp.status == 200:
                    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                        s.settimeout(1)
                        s.connect(("localhost", self.frontend_port))
                    return
            except Exception as e:  # noqa: BLE001 -- retry regardless of failure shape
                last_error = e
            if self.process.poll() is not None:
                output = "".join(self._output_lines)
                raise RuntimeError(f"Warp ({self._frontend_name}) process exited early "
                                    f"(code {self.process.returncode})\noutput:\n{output}")
            time.sleep(0.3)
        output = "".join(self._output_lines[-40:])
        raise TimeoutError(
            f"Warp ({self._frontend_name}) did not become ready in {timeout}s: {last_error}\n"
            f"last output:\n{output}")

    def metrics_text(self):
        conn = http.client.HTTPConnection("localhost", self.metrics_port, timeout=2)
        conn.request("GET", "/metrics")
        return conn.getresponse().read().decode("utf-8")

    def close(self):
        if self.process.poll() is None:
            self.process.terminate()
            try:
                self.process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.process.kill()


class RealMinio:
    """A real, disposable MinIO container (same quay.io image RealMinio.java uses) -- plain
    `docker run`, cleaned up in close() like RealPostgres."""

    IMAGE = "quay.io/minio/minio:latest"
    ACCESS_KEY = "warptestkey"
    SECRET_KEY = "warptestsecret"

    def __init__(self):
        self.name = f"warp-pytest-minio-{uuid.uuid4().hex[:12]}"
        self.port = docker_run_on_free_port(self.name, lambda port: [
            "-p", f"{port}:9000", "-e", f"MINIO_ROOT_USER={self.ACCESS_KEY}",
            "-e", f"MINIO_ROOT_PASSWORD={self.SECRET_KEY}", self.IMAGE, "server", "/data"])
        self._wait_ready()

    @property
    def endpoint(self):
        return f"http://localhost:{self.port}"

    def _wait_ready(self, timeout=60):
        deadline = time.time() + timeout
        while time.time() < deadline:
            try:
                conn = http.client.HTTPConnection("localhost", self.port, timeout=1)
                conn.request("GET", "/minio/health/ready")
                resp = conn.getresponse()
                resp.read()
                if resp.status == 200:
                    return
            except Exception:  # noqa: BLE001 -- retry until ready
                pass
            time.sleep(0.3)
        raise TimeoutError(f"MinIO container {self.name} did not become ready in {timeout}s")

    def close(self):
        subprocess.run(["docker", "rm", "-f", "-v", self.name], capture_output=True, text=True)


class RealMongo:
    """A real, disposable MongoDB container (mongo:7.0, no auth) -- plain `docker run`."""

    IMAGE = "mongo:7.0"

    def __init__(self):
        self.name = f"warp-pytest-mongo-{uuid.uuid4().hex[:12]}"
        self.port = docker_run_on_free_port(self.name, lambda port: ["-p", f"{port}:27017", self.IMAGE])
        self._wait_ready()

    def _wait_ready(self, timeout=60):
        deadline = time.time() + timeout
        while time.time() < deadline:
            r = subprocess.run(["docker", "exec", self.name, "mongosh", "--quiet", "--eval",
                                "db.adminCommand('ping').ok"], capture_output=True, text=True)
            if r.returncode == 0 and r.stdout.strip().endswith("1"):
                return
            time.sleep(0.5)
        raise TimeoutError(f"MongoDB container {self.name} did not become ready in {timeout}s")

    def close(self):
        subprocess.run(["docker", "rm", "-f", "-v", self.name], capture_output=True, text=True)


class RealDynamoDb:
    """A real, disposable DynamoDB Local container (amazon/dynamodb-local, in-memory)."""

    IMAGE = "amazon/dynamodb-local:latest"
    ACCESS_KEY = "fakeAccessKey"
    SECRET_KEY = "fakeSecretKey"
    REGION = "us-east-1"

    def __init__(self):
        self.name = f"warp-pytest-dynamo-{uuid.uuid4().hex[:12]}"
        self.port = docker_run_on_free_port(self.name, lambda port: [
            "-p", f"{port}:8000", self.IMAGE, "-jar", "DynamoDBLocal.jar", "-inMemory", "-sharedDb"])
        self._wait_ready()

    @property
    def endpoint(self):
        return f"http://localhost:{self.port}"

    def _wait_ready(self, timeout=60):
        deadline = time.time() + timeout
        while time.time() < deadline:
            try:
                conn = http.client.HTTPConnection("localhost", self.port, timeout=1)
                conn.request("GET", "/")
                conn.getresponse().read()
                return
            except Exception:  # noqa: BLE001 -- retry until ready
                time.sleep(0.3)
        raise TimeoutError(f"DynamoDB Local container {self.name} did not become ready in {timeout}s")

    def close(self):
        subprocess.run(["docker", "rm", "-f", "-v", self.name], capture_output=True, text=True)


class RealKafka:
    """A real, disposable single-node Kafka broker (apache/kafka, KRaft) -- plain `docker run`."""

    IMAGE = "apache/kafka:latest"

    def __init__(self):
        self.name = f"warp-pytest-kafka-{uuid.uuid4().hex[:12]}"
        self.port = docker_run_on_free_port(self.name, lambda port: [
            "-p", f"{port}:{port}",
            "-e", "KAFKA_PROCESS_ROLES=broker,controller", "-e", "KAFKA_NODE_ID=1",
            "-e", "KAFKA_CONTROLLER_QUORUM_VOTERS=1@localhost:9093",
            "-e", f"KAFKA_LISTENERS=PLAINTEXT://:{port},CONTROLLER://:9093",
            "-e", f"KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://localhost:{port}",
            "-e", "KAFKA_INTER_BROKER_LISTENER_NAME=PLAINTEXT", "-e", "KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER",
            self.IMAGE])
        self._wait_ready()

    @property
    def bootstrap_servers(self):
        return f"localhost:{self.port}"

    def _topics(self, *args):
        return subprocess.run(["docker", "exec", self.name, "/opt/kafka/bin/kafka-topics.sh",
                               "--bootstrap-server", f"localhost:{self.port}", *args],
                              capture_output=True, text=True)

    def _wait_ready(self, timeout=90):
        deadline = time.time() + timeout
        while time.time() < deadline:
            if self._topics("--list").returncode == 0:
                return
            time.sleep(1)
        raise TimeoutError(f"Kafka container {self.name} did not become ready in {timeout}s")

    def create_topic(self, topic):
        r = self._topics("--create", "--topic", topic, "--partitions", "1", "--replication-factor", "1")
        assert r.returncode == 0, r.stderr

    def close(self):
        subprocess.run(["docker", "rm", "-f", "-v", self.name], capture_output=True, text=True)


class RealCassandra:
    """A real, disposable single-node Cassandra 5 container (datacenter1) -- plain `docker run`."""

    IMAGE = "cassandra:5"
    LOCAL_DC = "datacenter1"

    def __init__(self):
        self.name = f"warp-pytest-cassandra-{uuid.uuid4().hex[:12]}"
        self.port = docker_run_on_free_port(self.name, lambda port: ["-p", f"{port}:9042", self.IMAGE])
        self._wait_ready()

    @property
    def contact_point(self):
        return f"localhost:{self.port}"

    def cql(self, statement):
        return subprocess.run(["docker", "exec", self.name, "cqlsh", "-e", statement], capture_output=True, text=True)

    def _wait_ready(self, timeout=240):
        deadline = time.time() + timeout
        while time.time() < deadline:
            if self.cql("SELECT release_version FROM system.local").returncode == 0:
                return
            time.sleep(3)
        raise TimeoutError(f"Cassandra container {self.name} did not become ready in {timeout}s")

    def close(self):
        subprocess.run(["docker", "rm", "-f", "-v", self.name], capture_output=True, text=True)
