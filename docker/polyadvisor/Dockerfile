# PolyAdvisor -- one image, one process. DmsHttpServer is built on embedded Jetty (already a
# dependency of this module) and now serves the built dms/web SPA itself via
# SpaResourceHandler -- no nginx or second container required. Build context is the repo root
# (see docker-compose.yml) since dms/ is a standalone Maven module (no parent pom, no
# reactor with wire/), so only dms/ ever needs to be in the build context.
#
# Prefer a separate static-file tier anyway (CDN-fronted, scaled independently of the API)?
# Dockerfile.frontend + nginx.conf are still here for that -- just don't set
# NEXAGRES_DMS_WEB_DIR on the backend and put your own static server in front instead.

# ---- build the SPA ----
FROM node:22-alpine AS web-build
WORKDIR /build
COPY dms/web/package.json dms/web/package-lock.json ./
RUN npm ci
COPY dms/web/ .
RUN npm run build

# ---- build the backend ----
FROM maven:3.9-eclipse-temurin-21 AS build
WORKDIR /build

# Dependency layer cached separately from source -- a source-only change (the common case) skips
# re-resolving the full dependency tree.
COPY dms/pom.xml .
RUN mvn -q -B dependency:go-offline

COPY dms/src ./src
RUN mvn -q -B -DskipTests package

# ---- runtime stage ----
FROM eclipse-temurin:21-jre-jammy
WORKDIR /app

RUN groupadd --system polyadvisor && useradd --system --gid polyadvisor --home-dir /app polyadvisor

COPY --from=build /build/target/nexagres-dms.jar /app/nexagres-dms.jar
COPY --from=web-build /build/dist /app/web

# NEXAGRES_DATA_DIR: embedded HSQLDB file store (ConnectionStore/LlmSettingsStore/ReportStore --
# saved connections, LLM provider config, uploaded performance reports) -- see those classes'
# javadoc. Defaults to ~/.nexagres; pointed at a dedicated, volume-mountable directory here so
# `docker-compose.yml` can persist it across container restarts without needing to know this
# app's home-directory default.
#
# NEXAGRES_DMS_WEB_DIR: tells DmsHttpServer to serve the SPA copied in above via its own
# embedded Jetty (SpaResourceHandler) -- unset this env var (or don't set it) to run API-only.
ENV NEXAGRES_DATA_DIR=/data
ENV NEXAGRES_DMS_WEB_DIR=/app/web
RUN mkdir -p /data && chown -R polyadvisor:polyadvisor /app /data
USER polyadvisor

VOLUME ["/data"]
EXPOSE 8090

ENTRYPOINT ["java", "-jar", "/app/nexagres-dms.jar"]
