# NexaDMS — Docker

One image, one container: `DmsHttpServer` runs on embedded Jetty (already a dependency of
this module) and serves the built `dms/web` SPA directly via `SpaResourceHandler`, alongside
its own JSON API — no nginx or second container needed.

## Quick start

From the **repo root** (the build context has to be the repo root — `dms/` is a standalone
Maven module with no parent pom, but the Dockerfile still needs `dms/` as a subdirectory it
can `COPY` from):

```bash
docker compose -f docker/dms/docker-compose.yml up --build
```

Open `http://localhost:8090`. The API and UI are on the same origin — `/api/*` is handled by
`DmsHttpServer`'s own routes, everything else (JS/CSS/images, and any path that doesn't
match a real static file, e.g. a client-side route like `/reports/42`) falls back to the SPA
via `SpaResourceHandler`, standard SPA-hosting behavior.

## Building the image standalone

```bash
docker build -f docker/dms/Dockerfile -t nexagres-dms:latest .
```

(Still run from the repo root — same reason as above.)

## How the SPA gets served (no nginx)

The image build has two stages that feed the runtime stage: a `node:22-alpine` stage builds
`dms/web` to static files, and a `maven:3.9-eclipse-temurin-21` stage builds the backend
jar. Both outputs land in the final `eclipse-temurin:21-jre-jammy` image; `NEXAGRES_DMS_WEB_DIR=/app/web`
tells `DmsHttpServer` where to find the built SPA at startup. Unset that env var (or point
it at a directory that doesn't exist) and the same image runs API-only — useful if you want to
front it with your own static tier or CDN instead.

**Prefer a separate static-file tier anyway?** (independently scaled, CDN-fronted, etc.)
`Dockerfile.frontend` and `nginx.conf` are still in this directory for that path — build them
the same way the old two-image setup did, and don't set `NEXAGRES_DMS_WEB_DIR` on the
backend container so it only serves the API.

## Data persistence

NexaDMS's own state (saved connections, LLM provider config, uploaded performance reports)
lives in an embedded HSQLDB file store at `NEXAGRES_DATA_DIR` (default `/data` in this image,
`~/.nexagres` outside a container — see `ConnectionStore`/`LlmSettingsStore`/`ReportStore`'s
javadoc). `docker-compose.yml` mounts this as a named volume (`polyadvisor-data`) so it survives
container restarts and rebuilds; delete the volume to start fresh.

## Testing against real source databases

`dms/docker-compose.test.yml` (not this directory) spins up real Oracle/MySQL/SQL Server
containers to point Connections at for live catalog profiling/workload capture testing — separate
from this app-runtime compose file, see that file's own header comment.
