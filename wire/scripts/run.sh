#!/usr/bin/env bash
# Runs polygres-wire's Main. Needs the --add-opens flags below on JDK 17+ (confirmed required on
# JDK 25 during live testing, 2026-08-17) because embedded Apache Ignite -- used by CacheStage,
# only actually started when POLYWIRE_CACHE_TABLES is set -- reflectively opens java.lang.reflect.Field
# on classes across several java.base packages via BinaryClassDescriptor, which the JVM's module
# system blocks by default from Java 17 onward. Without these flags the process throws
# InaccessibleObjectException during Ignite startup, but only when caching is actually enabled --
# a plain `java -jar target/polygres-wire.jar` run with the cache off (the default) works fine
# without any of this, which is why it wasn't caught until caching was live-tested.
#
# Usage: ./scripts/run.sh
# Config: all via env vars, see Main.java's javadoc and its startup log lines for the full list
# (ORAPG_PG_*/ORAPG_AUTH_*, POLYWIRE_*_PORT, POLYWIRE_QOS_*, POLYWIRE_CACHE_*, POLYWIRE_OTEL_ENDPOINT).

set -euo pipefail
cd "$(dirname "$0")/.."

exec java \
  --add-opens=java.base/jdk.internal.access=ALL-UNNAMED \
  --add-opens=java.base/jdk.internal.misc=ALL-UNNAMED \
  --add-opens=java.base/jdk.internal.ref=ALL-UNNAMED \
  --add-opens=java.base/java.lang=ALL-UNNAMED \
  --add-opens=java.base/java.lang.reflect=ALL-UNNAMED \
  --add-opens=java.base/java.io=ALL-UNNAMED \
  --add-opens=java.base/java.nio=ALL-UNNAMED \
  --add-opens=java.base/java.util=ALL-UNNAMED \
  --add-opens=java.base/java.util.concurrent=ALL-UNNAMED \
  --add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED \
  --add-opens=java.base/java.util.concurrent.locks=ALL-UNNAMED \
  --add-opens=java.base/java.math=ALL-UNNAMED \
  --add-opens=java.base/java.time=ALL-UNNAMED \
  --add-opens=java.base/java.text=ALL-UNNAMED \
  --add-opens=java.base/java.net=ALL-UNNAMED \
  --add-opens=java.sql/java.sql=ALL-UNNAMED \
  --add-opens=java.base/sun.nio.ch=ALL-UNNAMED \
  -jar target/polygres-wire.jar "$@"
