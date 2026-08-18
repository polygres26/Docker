# PolyAdvisor backend -- migration assessment/scoring HTTP server (AdvisorHttpServer). Build
# context is the repo root (see docker-compose.yml) since this Dockerfile COPYs the advisor/
# module in explicitly -- advisor/ is a standalone Maven module (no parent pom, no reactor with
# wire/), so only advisor/ ever needs to be in the build context.
#
# The frontend (advisor/web, a Vite/React SPA) is a *separate* image -- see Dockerfile.frontend --
# since this backend doesn't serve it directly yet (no SpaResourceHandler-equivalent ported here,
# see advisor/web/vite.config.ts's own comment on that gap). docker-compose.yml wires both
# together with nginx doing the proxying in production/compose, the same job Vite's dev-server
# proxy does in `npm run dev`.

# ---- build stage ----
FROM maven:3.9-eclipse-temurin-21 AS build
WORKDIR /build

# Dependency layer cached separately from source -- a source-only change (the common case) skips
# re-resolving the full dependency tree.
COPY advisor/pom.xml .
RUN mvn -q -B dependency:go-offline

COPY advisor/src ./src
RUN mvn -q -B -DskipTests package

# ---- runtime stage ----
FROM eclipse-temurin:21-jre-jammy
WORKDIR /app

RUN groupadd --system polyadvisor && useradd --system --gid polyadvisor --home-dir /app polyadvisor

COPY --from=build /build/target/polygres-advisor.jar /app/polygres-advisor.jar

# POLYGRES_DATA_DIR: embedded HSQLDB file store (ConnectionStore/LlmSettingsStore/ReportStore --
# saved connections, LLM provider config, uploaded performance reports) -- see those classes'
# javadoc. Defaults to ~/.polygres; pointed at a dedicated, volume-mountable directory here so
# `docker-compose.yml` can persist it across container restarts without needing to know this
# app's home-directory default.
ENV POLYGRES_DATA_DIR=/data
RUN mkdir -p /data && chown -R polyadvisor:polyadvisor /app /data
USER polyadvisor

VOLUME ["/data"]
EXPOSE 8090

ENTRYPOINT ["java", "-jar", "/app/polygres-advisor.jar"]
