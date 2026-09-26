# Warp -- multi-protocol gateway that makes Postgres speak SQL (pg/my/mssql/ora), MongoDB, DynamoDB,
# SQS, S3, Redis, OpenSearch, InfluxDB, Neo4j, Azure Storage, Google Cloud Storage, SNS, Kinesis,
# Secrets Manager, SSM, KMS, STS, Pub/Sub, Firestore, Datastore, Bigtable and Cassandra (CQL), plus gRPC and MCP, and its own admin app (Warp/web). Build context is the repo root
# (see docker-compose.yml / the `docker build` command in README.md) since this Dockerfile COPYs
# both Warp/ and Warp/web in explicitly -- Warp/ is a standalone Maven module (no parent pom, no
# reactor with advisor/), so only Warp/ ever needs to be in the build context, keeping rebuilds
# fast when advisor/ changes.

# ---- build the admin SPA ----
FROM node:22-alpine AS web-build
WORKDIR /build
COPY Warp/web/package.json Warp/web/package-lock.json ./
RUN npm ci
COPY Warp/web/ .
RUN npm run build

# ---- build the backend ----
FROM maven:3.9-eclipse-temurin-21 AS build
WORKDIR /build

# Dependency layer cached separately from source -- a source-only change (the common case) skips
# re-resolving the full dependency tree (real cost here: grpc/Calcite/Ignite/Nimbus pull a lot).
COPY Warp/pom.xml .
RUN mvn -q -B dependency:go-offline

COPY Warp/src ./src
RUN mvn -q -B -DskipTests package

# ---- runtime stage ----
# Same JDK line as the build stage (pom.xml targets 21) -- jre, not jdk: this project ships a
# shaded jar with every dependency already bundled, nothing here ever needs javac/jar/etc.
FROM eclipse-temurin:21-jre-jammy
WORKDIR /app

RUN groupadd --system warp && useradd --system --gid warp --home-dir /app warp

# /app itself (the directory WORKDIR created, owned by root) needs to be writable by warp at
# RUNTIME too, not just its contents readable -- WarpCluster creates ignite/work under here on
# startup whenever caching is enabled (WARP_CACHE_TABLES), and that mkdir fails without it
# (found live: "Work directory does not exist and cannot be created" -- a real regression the
# --chown-only version below introduced, since --chown on a COPY only touches the files it copies
# in, never the pre-existing parent directory). This chown is cheap (an empty directory inode, not
# a large file) so it doesn't reintroduce the layer-duplication cost this whole change is about
# avoiding.
RUN chown warp:warp /app

# --chown here, not a separate `RUN chown -R /app` after -- that used to write a whole SECOND
# ~111MB layer (Docker's layer model can't patch ownership in place; changing it after the fact
# means a full new copy of everything under /app), doubling this jar's real footprint in the
# image for zero benefit. --chown sets ownership as part of the same COPY layer instead -- same
# final files, same owner, same permissions, just without paying for them twice.
COPY --from=build --chown=warp:warp /build/target/sayonora-wire.jar /app/sayonora-wire.jar
COPY --from=web-build --chown=warp:warp /build/dist /app/web

# WARP_ADMIN_WEB_DIR: tells MetricsServer to serve the built admin SPA copied in above via
# its own embedded Jetty (SpaResourceHandler) on the admin/metrics port -- unset this env var to
# run API-only (the JSON admin API still works either way; this only controls whether the browser
# UI is also served on the same port).
ENV WARP_ADMIN_WEB_DIR=/app/web
USER warp

# Every TCP/HTTP frontend Warp can start -- see ServerOptions.java/Main.java for the full,
# authoritative list and each one's real default. Not all are reachable unless published (see
# docker-compose.yml's own port list for which ones this compose setup actually exposes).
# SQL: 15432 pg, 13306 my, 14333 mssql, 11521 ora (2484 ora TLS). Stores: 27017 mongo, 18000 dynamo,
# 16379 redis, 9200 opensearch, 8086 influx, 7687 bolt, 9324 sqs, 18020 s3, 4443 gcs, 4566 aws (sns,
# kinesis, secrets, ssm, kms, sts), 8085 pubsub gRPC, 8088 bigtable, 8080 firestore, 8081 datastore, 19042 cassandra (CQL), 8182 gremlin (TinkerPop Gremlin Server: WebSocket + HTTP), 19092 kafka. Native: 7070/17071 gRPC, 18010 MCP, 19090 admin.
EXPOSE 15432 13306 11521 2484 14333 27017 7070 17071 18000 9324 9200 18010 19090 16379 8086 7687 18020 4443 4566 8085 8087 8088 8080 8081 19042 18030 19092 8182

# --add-opens flags: required by embedded Apache Ignite (CacheStage, only actually started when
# WARP_CACHE_TABLES is set) reflectively opening java.lang.reflect.Field on several java.base
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
    "-jar", "/app/sayonora-wire.jar"]
