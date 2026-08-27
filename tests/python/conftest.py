"""Connection details for a PolyWire instance already running (via ../docker-compose.test.yml,
or your own `docker run`) -- no fixtures start/stop anything here, unlike wire's own private
integration tests, since these are meant to run against the public, prebuilt image with no
source checkout. Override POLYWIRE_HOST if not running via the compose file's default port
mapping on localhost.
"""
import os

HOST = os.environ.get("POLYWIRE_HOST", "localhost")

PGWIRE_PORT = int(os.environ.get("POLYWIRE_PGWIRE_PORT", "15432"))
MYWIRE_PORT = int(os.environ.get("POLYWIRE_MYWIRE_PORT", "13306"))
ORAWIRE_PORT = int(os.environ.get("POLYWIRE_ORAWIRE_PORT", "11521"))
MSSQLWIRE_PORT = int(os.environ.get("POLYWIRE_MSSQLWIRE_PORT", "14333"))
MONGOWIRE_PORT = int(os.environ.get("POLYWIRE_MONGOWIRE_PORT", "27017"))
DYNAMOWIRE_PORT = int(os.environ.get("POLYWIRE_DYNAMOWIRE_PORT", "18000"))
SQSWIRE_PORT = int(os.environ.get("POLYWIRE_SQSWIRE_PORT", "9324"))
OSWIRE_PORT = int(os.environ.get("POLYWIRE_OSWIRE_PORT", "9200"))
INFLUXWIRE_PORT = int(os.environ.get("POLYWIRE_INFLUXWIRE_PORT", "8086"))
BOLTWIRE_PORT = int(os.environ.get("POLYWIRE_BOLTWIRE_PORT", "7687"))
