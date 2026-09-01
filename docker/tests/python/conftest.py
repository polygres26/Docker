"""Connection details for a Warp instance already running (via ../docker-compose.test.yml,
or your own `docker run`) -- no fixtures start/stop anything here, unlike wire's own private
integration tests, since these are meant to run against the public, prebuilt image with no
source checkout. Override WARP_HOST if not running via the compose file's default port
mapping on localhost.
"""
import os

HOST = os.environ.get("WARP_HOST", "localhost")

PGWIRE_PORT = int(os.environ.get("WARP_PGWIRE_PORT", "15432"))
MYWIRE_PORT = int(os.environ.get("WARP_MYWIRE_PORT", "13306"))
ORAWIRE_PORT = int(os.environ.get("WARP_ORAWIRE_PORT", "11521"))
MSSQLWIRE_PORT = int(os.environ.get("WARP_MSSQLWIRE_PORT", "14333"))
MONGOWIRE_PORT = int(os.environ.get("WARP_MONGOWIRE_PORT", "27017"))
DYNAMOWIRE_PORT = int(os.environ.get("WARP_DYNAMOWIRE_PORT", "18000"))
SQSWIRE_PORT = int(os.environ.get("WARP_SQSWIRE_PORT", "9324"))
OSWIRE_PORT = int(os.environ.get("WARP_OSWIRE_PORT", "9200"))
INFLUXWIRE_PORT = int(os.environ.get("WARP_INFLUXWIRE_PORT", "8086"))
BOLTWIRE_PORT = int(os.environ.get("WARP_BOLTWIRE_PORT", "7687"))
