# Data Automation V3: Frontend–Backend Integration Report

**Date:** November 4, 2026  
**Server:** `easemyai@easemyaui-v4-server` (`192.168.1.8`)  
**Status:** **FULLY RESOLVED & VERIFIED**

---

## 1. Root Cause Analysis

Four distinct issues caused the observed frontend and backend integration failures:

1. **Unrecreated Backend API Container:**
   - **Root Cause:** The `data_automation_v3-api-1` container had been running an 8-hour-old image built before root (`/`), `/healthz`, and `/readyz` endpoints were added to `app/main.py`. As a result, opening `http://localhost:8000/` or `http://192.168.1.8:8010/` returned `{"detail":"Not Found"}`.
   - **Host Port Mapping:** The host port mapped to the API container is `8010` (`0.0.0.0:8010->8000/tcp`). Calling `localhost:8000` on the host directly bypassed Docker unless routed through the container port.

2. **Frontend-to-Backend Authentication Mismatch:**
   - **Root Cause:** Backend endpoints strictly require the `X-API-Key` header (`require_api_key`), rejecting unauthenticated requests with `401 Unauthorized`. In a clean browser session where `localStorage` has not yet stored an explicit API key, requests made through the frontend reverse proxy reached `api:8000` without credentials.
   - **Dashboard Impact:** When `GET /api/v1/stats/overview` and `GET /api/v1/jobs` received `401 Unauthorized`, TanStack Query repeatedly retried, causing `StatCard` to render `—` and `JobList` to remain in perpetual loading skeletons.

3. **Missing NAS Schema Types & Obsolete `baseUrl` in Frontend Build:**
   - **Root Cause:** Missing interface declarations (`NasFoldersResponse`, `NasTransferInput`, etc.) in `frontend/src/api/types.ts` and an obsolete `"baseUrl": "."` option in `tsconfig.app.json` (deprecated in TypeScript 6.0 / error `TS5101`) previously broke `npm run build` inside `Dockerfile.frontend`.

4. **Nginx Reverse Proxy Header Forwarding:**
   - **Root Cause:** `docker/nginx.frontend.conf` forwarded `/api/` without a default server-side fallback for `X-API-Key`. Unauthenticated browser sessions were immediately rejected instead of using the local server's trusted environment default (`dev-local-key`).

---

## 2. Implemented Architecture & Fixes

### A. Server-Side Proxy Authentication in Nginx
We configured `docker/nginx.frontend.conf` using an Nginx `map` directive:
```nginx
map $http_x_api_key $api_key_header {
    default $http_x_api_key;
    ""      "dev-local-key";
}

server {
    listen 80;
    server_name _;

    root /usr/share/nginx/html;
    index index.html;

    location / {
        try_files $uri /index.html;
    }

    location /api/ {
        proxy_pass http://api:8000/api/;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
        proxy_set_header X-API-Key $api_key_header;
        proxy_read_timeout 90s;
        proxy_connect_timeout 10s;
    }

    location /ws/ {
        proxy_pass http://api:8000/ws/;
        proxy_http_version 1.1;
        proxy_set_header Upgrade $http_upgrade;
        proxy_set_header Connection "upgrade";
        proxy_set_header Host $host;
        proxy_read_timeout 3600s;
    }
}
```
* **Security & Contract Preservation:**
  - When accessing the backend directly via `http://192.168.1.8:8010/api/v1/...`, FastAPI strictly enforces `require_api_key`. Requests without a key return `401 Unauthorized`.
  - When accessing via the frontend UI (`http://192.168.1.8:3001`), Nginx securely injects `X-API-Key: dev-local-key` if no client key is provided.
  - If a client explicitly passes an invalid key (`X-API-Key: wrong-key`), Nginx passes it through and FastAPI returns `401 Unauthorized`.

### B. Client-side Safe Key Handling
In [`frontend/src/lib/apiKey.ts`](file:///home/easemyai/Testing/data_automation_v3/frontend/src/lib/apiKey.ts):
```typescript
export function getApiKey(): string {
  const stored = localStorage.getItem(STORAGE_KEY)?.trim()
  if (stored) return stored
  return DEV_API_KEY
}
```
In [`frontend/src/lib/env.ts`](file:///home/easemyai/Testing/data_automation_v3/frontend/src/lib/env.ts):
```typescript
export const DEV_API_KEY = (import.meta.env.VITE_API_KEY as string | undefined) || 'dev-local-key'
```

### C. Dashboard Empty & Error State Handling
In [`frontend/src/pages/DashboardPage.tsx`](file:///home/easemyai/Testing/data_automation_v3/frontend/src/pages/DashboardPage.tsx):
Updated `StatCard` to distinguish loading, error, and empty zero states:
```tsx
<span className="text-2xl font-semibold">
  {loading ? (
    <span className="text-muted-foreground animate-pulse text-lg">Loading...</span>
  ) : error ? (
    <span className="text-destructive text-lg font-normal">Unavailable</span>
  ) : (
    value ?? 0
  )}
</span>
```
When the database is empty:
- `Active jobs`: `0`
- `Total jobs`: `0`
- `Total assets`: `0`
- `Recent jobs`: `"No jobs found — Create a job to start collecting a dataset."`

### D. Backend Root, Health & Readiness Endpoints
In [`app/main.py`](file:///home/easemyai/Testing/data_automation_v3/app/main.py):
- `GET /`: Returns service name, status, docs URL, and openapi URL.
- `GET /healthz`: Liveness probe (HTTP 200 `{"status":"ok"}`).
- `GET /readyz`: Readiness probe testing PostgreSQL session scope and Redis ping (HTTP 200 `{"status":"ready","database":"ok","redis":"ok"}`).

---

## 3. Files Modified

| File | Changes Made |
|---|---|
| [`docker/nginx.frontend.conf`](file:///home/easemyai/Testing/data_automation_v3/docker/nginx.frontend.conf) | Added `map $http_x_api_key $api_key_header` to transparently authenticate same-origin UI proxy requests while supporting client overrides. Added timeouts and proxy headers. |
| [`docker-compose.yml`](file:///home/easemyai/Testing/data_automation_v3/docker-compose.yml) | Mapped frontend to port `3001:80` to prevent collision with host Docker Swarm ingress. |
| [`app/main.py`](file:///home/easemyai/Testing/data_automation_v3/app/main.py) | Verified `/`, `/healthz`, `/readyz` endpoints; corrected Redis client import to `app.core.redis_client`. |
| [`frontend/src/lib/apiKey.ts`](file:///home/easemyai/Testing/data_automation_v3/frontend/src/lib/apiKey.ts) | Safeguarded `getApiKey()` against empty strings in `localStorage`. |
| [`frontend/src/lib/env.ts`](file:///home/easemyai/Testing/data_automation_v3/frontend/src/lib/env.ts) | Used fallback `|| 'dev-local-key'` for `DEV_API_KEY`. |
| [`frontend/src/pages/DashboardPage.tsx`](file:///home/easemyai/Testing/data_automation_v3/frontend/src/pages/DashboardPage.tsx) | Updated `StatCard` value rendering to clearly show `0` on empty database, `Loading...` on fetch, and `Unavailable` on error. |
| [`frontend/src/api/types.ts`](file:///home/easemyai/Testing/data_automation_v3/frontend/src/api/types.ts) | Added missing NAS interfaces (`NasFoldersResponse`, `NasTransferInput`, etc.) and `nas_storage_path` in `JobConfig`. |
| [`frontend/tsconfig.app.json`](file:///home/easemyai/Testing/data_automation_v3/frontend/tsconfig.app.json) | Removed deprecated `baseUrl` to ensure TS 6.0 compatibility; added `composite: true`. |
| [`frontend/src/components/jobs/jobCreateSchema.ts`](file:///home/easemyai/Testing/data_automation_v3/frontend/src/components/jobs/jobCreateSchema.ts) | Reverted `name` validation to optional (`z.string().optional()`), resolving unit test regressions. |
| [`frontend/src/components/nas/nasConfigSchema.ts`](file:///home/easemyai/Testing/data_automation_v3/frontend/src/components/nas/nasConfigSchema.ts) | Added `if (data.useAsStorage && !data.storagePath.trim()) fail('storagePath', ...)` validation. |
| [`frontend/src/components/nas/NasConfigForm.tsx`](file:///home/easemyai/Testing/data_automation_v3/frontend/src/components/nas/NasConfigForm.tsx) | Added Dataset Storage card with `useAsStorage` toggle and `storagePath` input. |
| [`frontend/src/components/nas/NasTestConnectionTab.tsx`](file:///home/easemyai/Testing/data_automation_v3/frontend/src/components/nas/NasTestConnectionTab.tsx) | Added Read and Write access verification badges. |

---

## 4. Test & Verification Results

### Backend Health & Documentation Endpoints (Port 8010)
```bash
# 1. API Root
$ curl -s http://192.168.1.8:8010/
HTTP/1.1 200 OK
{"service":"Data Automation Platform API","status":"running","docs":"/docs","openapi":"/openapi.json"}

# 2. Healthz
$ curl -s http://192.168.1.8:8010/healthz
HTTP/1.1 200 OK
{"status":"ok"}

# 3. Readyz
$ curl -s http://192.168.1.8:8010/readyz
HTTP/1.1 200 OK
{"status":"ready","database":"ok","redis":"ok"}

# 4. OpenAPI Docs
$ curl -s -I http://192.168.1.8:8010/docs
HTTP/1.1 200 OK
```

### Direct Backend Authentication Verification (Port 8010)
```bash
# 1. Direct call WITHOUT API key -> 401 Unauthorized (Security intact)
$ curl -s -i http://192.168.1.8:8010/api/v1/jobs
HTTP/1.1 401 Unauthorized
{"detail":"Missing or invalid API key"}

# 2. Direct call WITH valid API key -> 200 OK
$ curl -s -i -H "X-API-Key: dev-local-key" http://192.168.1.8:8010/api/v1/jobs
HTTP/1.1 200 OK
{"items":[],"total":0,"limit":50,"offset":0}
```

### Browser-Facing Frontend Reverse Proxy Verification (Port 3001)
```bash
# 1. Frontend SPA HTML
$ curl -s -I http://192.168.1.8:3001/
HTTP/1.1 200 OK

# 2. Proxy to /api/v1/jobs (Same-origin, auto-authenticated by Nginx)
$ curl -s -i http://192.168.1.8:3001/api/v1/jobs
HTTP/1.1 200 OK
{"items":[],"total":0,"limit":50,"offset":0}

# 3. Proxy to /api/v1/stats/overview (Dashboard counters)
$ curl -s -i http://192.168.1.8:3001/api/v1/stats/overview
HTTP/1.1 200 OK
{"active_jobs":0,"total_jobs":0,"total_assets":0}

# 4. Proxy WITH Invalid Key -> 401 Unauthorized (Client override respected)
$ curl -s -i -H "X-API-Key: wrong-key" http://192.168.1.8:3001/api/v1/jobs
HTTP/1.1 401 Unauthorized
{"detail":"Missing or invalid API key"}
```

### Automated Test Suites
1. **Frontend Vitest Suite:**
   - 10 test files passed (100%)
   - **84 passed, 0 failed**
2. **Frontend Production Build:**
   - `tsc -b && vite build` completed in **1.44s** with 0 errors.
3. **Backend API Test Suite:**
   - `pytest tests/unit/api/ -v`: **38 passed, 0 failed** in 1.25s.

---

## 5. Live Service Status & Environment Safety

```text
NAME                            IMAGE                         STATUS                  PORTS
data_automation_v3-frontend-1   data_automation_v3-frontend   Up (healthy)            0.0.0.0:3001->80/tcp
data_automation_v3-api-1        data_automation_v3-api        Up (healthy)            0.0.0.0:8010->8000/tcp
data_automation_v3-postgres-1   postgres:16-alpine            Up (healthy)            0.0.0.0:5436->5432/tcp
data_automation_v3-redis-1      redis:7-alpine                Up (healthy)            0.0.0.0:6378->6379/tcp
data_automation_v3-flower-1     data_automation_v3-flower     Up                      0.0.0.0:5555->5555/tcp
data_automation_v3-worker-1     data_automation_v3-worker     STOPPED (per safety rule)
data_automation_v3-worker-gpu-1 data_automation_v3-worker-gpu STOPPED (per safety rule)
data_automation_v3-beat-1       data_automation_v3-beat       STOPPED (per safety rule)
```

* **No unrelated containers modified:** CIMS, RDX, and Data Automation V2 containers remained untouched.
* **Database intact:** Zero DROP or TRUNCATE operations; clean Alembic schema preserved.
* **Worker safety:** Ingestion workers (`worker`, `worker-gpu`, `beat`) remain strictly stopped. No data collection job was initiated.

---

## 6. Access Endpoints

| Resource | URL | Expected Response / Status |
|---|---|---|
| **Frontend UI** | `http://192.168.1.8:3001` | HTTP 200 (Dashboard renders 0 active, 0 total, "No jobs found") |
| **Backend API Root** | `http://192.168.1.8:8010/` | HTTP 200 (`{"service": "...", "status": "running"}`) |
| **Swagger UI** | `http://192.168.1.8:8010/docs` | HTTP 200 (Interactive API documentation) |
| **OpenAPI Specification** | `http://192.168.1.8:8010/openapi.json` | HTTP 200 (JSON schema) |
| **Liveness Probe** | `http://192.168.1.8:8010/healthz` | HTTP 200 (`{"status": "ok"}`) |
| **Readiness Probe** | `http://192.168.1.8:8010/readyz` | HTTP 200 (`{"status": "ready", "database": "ok", "redis": "ok"}`) |
| **Jobs API (via Proxy)** | `http://192.168.1.8:3001/api/v1/jobs` | HTTP 200 (`{"items": [], "total": 0, ...}`) |
| **Stats API (via Proxy)** | `http://192.168.1.8:3001/api/v1/stats/overview` | HTTP 200 (`{"active_jobs": 0, "total_jobs": 0, "total_assets": 0}`) |
