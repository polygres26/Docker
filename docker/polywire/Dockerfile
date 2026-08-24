# PolyWire -- multi-protocol Postgres wire gateway (orawire/pgwire/mywire/mssqlwire/mongowire/
# dynamowire/sqswire/gRPC/MCP) plus its own admin app (wire/web). Build context is the repo root
# (see docker-compose.yml / the `docker build` command in README.md) since this Dockerfile COPYs
# both wire/ and wire/web in explicitly -- wire/ is a standalone Maven module (no parent pom, no
# reactor with advisor/), so only wire/ ever needs to be in the build context, keeping rebuilds
# fast when advisor/ changes.

# ---- build the admin SPA ----
FROM node:22-alpine AS web-build
WORKDIR /build
COPY wire/web/package.json wire/web/package-lock.json ./
RUN npm ci
COPY wire/web/ .
RUN npm run build

# ---- build the backend ----
FROM maven:3.9-eclipse-temurin-21 AS build
WORKDIR /build

# Dependency layer cached separately from source -- a source-only change (the common case) skips
# re-resolving the full dependency tree (real cost here: grpc/Calcite/Ignite/Nimbus pull a lot).
COPY wire/pom.xml .
RUN mvn -q -B dependency:go-offline

COPY wire/src ./src
RUN mvn -q -B -DskipTests package

# ---- runtime stage ----
# Same JDK line as the build stage (pom.xml targets 21) -- jre, not jdk: this project ships a
# shaded jar with every dependency already bundled, nothing here ever needs javac/jar/etc.
FROM eclipse-temurin:21-jre-jammy
WORKDIR /app

RUN groupadd --system polywire && useradd --system --gid polywire --home-dir /app polywire

COPY --from=build /build/target/polygres-wire.jar /app/polygres-wire.jar
COPY --from=web-build /build/dist /app/web

# POLYWIRE_ADMIN_WEB_DIR: tells MetricsServer to serve the built admin SPA copied in above via
# its own embedded Jetty (SpaResourceHandler) on the admin/metrics port -- unset this env var to
# run API-only (the JSON admin API still works either way; this only controls whether the browser
# UI is also served on the same port).
ENV POLYWIRE_ADMIN_WEB_DIR=/app/web
RUN chown -R polywire:polywire /app
USER polywire

# Every TCP/HTTP frontend PolyWire can start -- see ServerOptions.java/Main.java for the full,
# authoritative list and each one's real default. Not all are reachable unless published (see
# docker-compose.yml's own port list for which ones this compose setup actually exposes).
EXPOSE 15432 13306 11521 2484 14333 27017 7070 17071 18000 18010 19090

# --add-opens flags: required by embedded Apache Ignite (CacheStage, only actually started when
# POLYWIRE_CACHE_TABLES is set) reflectively opening java.lang.reflect.Field on several java.base
# packages -- blocked by the JPMS module system from Java 17 onward without these. Harmless no-ops
# when caching is off (the default); see scripts/run.sh's own javadoc-style comment for the exact
# same flag set and how this was actually found (InaccessibleObjectException during live testing,
# JDK 25, only once caching was enabled -- kept here unconditionally rather than only-when-caching
# since there's no reasonable way to conditionally alter JVM flags from inside a Dockerfile ENTRYPOINT
# based on an env var without an extra shell wrapper, and the flags are free when unused).
ENTRYPOINT ["java", \
    "--add-opens=java.base/jdk.internal.access=ALL-UNNAMED", \
    "--add-opens=java.base/jdk.internal.misc=ALL-UNNAMED", \
    "--add-opens=java.base/jdk.internal.ref=ALL-UNNAMED", \
    "--add-opens=java.base/java.lang=ALL-UNNAMED", \
    "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED", \
    "--add-opens=java.base/java.io=ALL-UNNAMED", \
    "--add-opens=java.base/java.nio=ALL-UNNAMED", \
    "--add-opens=java.base/java.util=ALL-UNNAMED", \
    "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED", \
    "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED", \
    "--add-opens=java.base/java.util.concurrent.locks=ALL-UNNAMED", \
    "--add-opens=java.base/java.math=ALL-UNNAMED", \
    "--add-opens=java.base/java.time=ALL-UNNAMED", \
    "--add-opens=java.base/java.text=ALL-UNNAMED", \
    "--add-opens=java.base/java.net=ALL-UNNAMED", \
    "--add-opens=java.sql/java.sql=ALL-UNNAMED", \
    "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED", \
    "-jar", "/app/polygres-wire.jar"]
