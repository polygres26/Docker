# PolyAdvisor — Docker

Two images:

- **`Dockerfile`** — the Java backend (`AdvisorHttpServer`). Multi-stage: `maven:3.9-eclipse-
  temurin-21` builds the shaded jar (`target/polygres-advisor.jar`), then an
  `eclipse-temurin:21-jre-jammy` runtime stage runs it.
- **`Dockerfile.frontend`** — the `advisor/web` Vite/React SPA, built to static files (`node:22-
  alpine`) and served by nginx (`nginx.conf`), which also proxies `/api` to the backend container.
  Two separate images because the backend doesn't serve the built SPA directly yet — see
  `advisor/web/vite.config.ts`'s own comment on that gap (a `SpaResourceHandler`-equivalent isn't
  ported here); nginx does the same job Vite's own dev-server proxy does for `npm run dev`.

## Quick start

From the **repo root** (the build context has to be the repo root — `advisor/` is a standalone
Maven module with no parent pom, but both Dockerfiles still need `advisor/` as a subdirectory they
can `COPY` from):

```bash
docker compose -f docker/polyadvisor/docker-compose.yml up --build
```

Open `http://localhost:8080`. The backend itself isn't published on the host — everything goes
through nginx on 8080, matching how this is meant to be deployed (one public entry point).

## Building the images standalone

```bash
docker build -f docker/polyadvisor/Dockerfile -t polyadvisor-backend:latest .
docker build -f docker/polyadvisor/Dockerfile.frontend -t polyadvisor-frontend:latest .
```

(Still run from the repo root — same reason as above.)

## Data persistence

PolyAdvisor's own state (saved connections, LLM provider config, uploaded performance reports)
lives in an embedded HSQLDB file store at `POLYGRES_DATA_DIR` (default `/data` in this image,
`~/.polygres` outside a container — see `ConnectionStore`/`LlmSettingsStore`/`ReportStore`'s
javadoc). `docker-compose.yml` mounts this as a named volume (`polyadvisor-data`) so it survives
container restarts and rebuilds; delete the volume to start fresh.

## Testing against real source databases

`advisor/docker-compose.test.yml` (not this directory) spins up real Oracle/MySQL/SQL Server
containers to point Connections at for live catalog profiling/workload capture testing — separate
from this app-runtime compose file, see that file's own header comment.
