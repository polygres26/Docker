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
JAR_PATH = os.path.join(REPO_ROOT, "target", "nexagres-wire.jar")

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


class RealPostgres:
    """A real, disposable Postgres container -- plain `docker run`, not a test-library
    abstraction, so it needs nothing beyond Docker itself being installed."""

    def __init__(self):
        self.name = f"warp-pytest-pg-{uuid.uuid4().hex[:12]}"
        self.port = free_port()
        subprocess.run(
            [
                "docker", "run", "-d", "--name", self.name,
                "-p", f"{self.port}:5432",
                "-e", "POSTGRES_USER=postgres",
                "-e", "POSTGRES_PASSWORD=postgres",
                "-e", "POSTGRES_DB=postgres",
                "postgres:16-alpine",
            ],
            check=True, capture_output=True, text=True,
        )
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
        subprocess.run(["docker", "rm", "-f", self.name], capture_output=True, text=True)


class WarpProcess:
    """A real Warp process (the shaded jar), pointed at a real Postgres backend."""

    def __init__(self, postgres: RealPostgres, frontend_env_var: str, frontend_name="frontend"):
        if not os.path.exists(JAR_PATH):
            raise RuntimeError(
                f"{JAR_PATH} not found -- run `mvn -DskipTests package` in wire/ before these tests")

        self.frontend_port = free_port()
        self.metrics_port = free_port()
        self._frontend_name = frontend_name

        env = dict(os.environ)
        env.update({
            "WARP_PG_HOST": "localhost",
            "WARP_PG_PORT": str(postgres.port),
            "WARP_PG_DATABASE": "postgres",
            "WARP_PG_USER": "postgres",
            "WARP_PG_PASSWORD": "postgres",
            "WARP_AUTH_USER": "postgres",
            "WARP_AUTH_PASSWORD": "postgres",
            "WARP_METRICS_PORT": str(self.metrics_port),
            # Default QoS admission control (rate=5/s burst=5, maxWaitMs=0 -- no queueing) is tuned
            # for production traffic shaping, not a test client's rapid connection-setup handshake;
            # without this a driver's own setup queries alone can trip "rate limit exceeded".
            "WARP_QOS_RATE_PER_SEC": "1000",
            "WARP_QOS_BURST": "1000",
            frontend_env_var: str(self.frontend_port),
        })

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
        self._wait_ready()

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
