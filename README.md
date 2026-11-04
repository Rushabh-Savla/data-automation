# Data Automation Platform V3

A production-grade, distributed data collection, filtering, and dataset management platform with automated multi-source ingestion, hardware-accelerated relevance filtering (SmolVLM / YOLO-World / CLIP), perceptual deduplication, transactional NAS offload, and real-time dashboard analytics.

---

## 1. System Architecture

```mermaid
flowchart TD
    User([User / Browser]) <-->|HTTP / WS| Frontend[Frontend React SPA\nPort 3001]
    Frontend <-->|REST API / WS| API[FastAPI Backend\nPort 8010]
    
    API <--> Postgres[(PostgreSQL 16\nPort 5436)]
    API <--> Redis[(Redis 7\nPort 6378)]
    
    Beat[Celery Beat Scheduler] -->|Periodic Tasks| Redis
    
    Redis -->|Queues: discovery, download, standardize, cleanup, orchestration, export| Worker[Celery IO Worker\n8 Concurrency]
    Redis -->|Queue: filtering| WorkerGPU[Celery GPU Worker\nRTX A4000 / CUDA]
    
    Worker -->|Fetch| Sources[Connectors:\nRoboflow, Kaggle, COCO, OpenImages,\nPascal VOC, SUN RGB-D, Visual Genome, Web, DAVIS]
    Worker -->|Staging| SSD[(Local SSD Staging\n/work/staging)]
    
    WorkerGPU -->|Infer & Filter| SSD
    Worker -->|Upload & Verify| NAS[(Remote Storage NAS\nSFTP: 192.168.1.178:2222)]
```

### End-to-End Pipeline Stages

1. **Discovery & Sharding**: Source tasks discover candidate items from configured sources (Roboflow, Kaggle, Web, COCO, etc.).
2. **Admission & Global Windowing**: Candidate items are admitted through durable Redis reservation limits ensuring concurrent active candidates do not saturate local disk.
3. **Download to Local Staging**: Media is fetched into `/work/staging/job_<id>/batch_<n>/window_<m>/`.
4. **Standardization & Promotion**: Standardizes images, normalizes bounding box annotations, extracts perceptual hashes (pHash), computes CLIP embeddings, and promotes drafts into `Asset` records.
5. **GPU Relevance & Perceptual Deduplication**: GPU worker evaluates relevance (SmolVLM/YOLO-World) and deduplicates against existing embeddings using cosine similarity.
6. **NAS Verified Upload**:
   - `NasWriterLock` guarantees global upload concurrency = 1 across cluster.
   - Files are uploaded as `.part` files over single reused SFTP sessions.
   - Remote file size is verified.
   - Remote SHA-256 checksum is computed and validated against original digest.
   - Atomic server-side publish/rename to final path.
7. **Local Staging Cleanup**: Local SSD files are deleted **only** after remote verification is confirmed and committed to PostgreSQL.
8. **Real-Time Dashboards & Metrics**: PostgreSQL counters and Redis WebSocket pub/sub stream live progress to the frontend UI.

---

## 2. Service Port Mappings

| Service | Internal Port | Host Port | Protocol | Description |
|---|---|---|---|---|
| **frontend** | `80` | `3001` | HTTP | React SPA (Vite / Tailwind / Shadcn UI) |
| **api** | `8000` | `8010` | HTTP / WS | FastAPI Application & Swagger Docs |
| **postgres** | `5432` | `5436` | TCP | PostgreSQL 16 (`data_automation` DB) |
| **redis** | `6379` | `6378` | TCP | Redis 7 (Broker, Cache, Locks) |
| **flower** | `5555` | `5555` | HTTP | Celery Task Monitoring Dashboard |
| **worker** | — | — | Internal | Celery IO Worker (8 concurrency) |
| **worker-gpu**| — | — | Internal | Celery GPU Worker (CUDA / RTX A4000) |
| **beat** | — | — | Internal | Celery Periodic Scheduler |

> **Note on Port Safety**: PostgreSQL uses host port **5436** to avoid colliding with `data_automation_v2` (running on 5435) or system PostgreSQL (5432).

---

## 3. Environment & Configuration

Environment configuration is read from `.env` in the project root:

```ini
# Database
DATABASE_URL=postgresql+psycopg://postgres:postgres@localhost:5436/data_automation
POSTGRES_PORT=5436

# Redis
REDIS_URL=redis://localhost:6378/0
REDIS_PORT=6378

# Storage Mounts
NAS_ROOT=/home/easemyai/Testing/data_automation_v3/nas_data
WORK_ROOT=/home/easemyai/Testing/data_automation_v3/work_data

# API Security
API_KEYS=dev-local-key
API_PORT=8010
FRONTEND_PORT=3001

# Worker Concurrency & Hardware
CELERY_WORKER_CONCURRENCY=8
TORCH_NUM_THREADS=2
TORCH_CUDA_MEMORY_FRACTION=0.35
```

---

## 4. Startup & Operations Guide

### Starting the Platform
```bash
# Start all 8 core services in background
docker compose up -d

# Verify all containers are healthy and running
docker compose ps
```

### Checking Logs
```bash
# API logs
docker compose logs -f api

# Celery IO worker logs
docker compose logs -f worker

# Celery GPU worker logs
docker compose logs -f worker-gpu

# Periodic Beat logs
docker compose logs -f beat
```

### Running Test Suites
```bash
# Backend pytest suite (903+ unit tests)
LOCAL_STAGING_PATH=/tmp/data-automation-test-staging \
LOCAL_STAGING_MAX_BYTES=53687091200 \
uv run --no-sync pytest tests/unit/ -v --tb=short

# Frontend Vitest suite (84 tests)
cd frontend && npx vitest run
```

---

## 5. Troubleshooting & Reliability Notes

1. **NAS Cooldown Recovery**:
   If the remote SFTP or SMB storage experiences network timeouts, a 180s protective cooldown is engaged to prevent connection flooding. To reset cooldown manually:
   ```bash
   docker compose exec redis redis-cli del nas_offload:cooldown
   ```

2. **Re-triggering NAS Offload Queue**:
   To manually trigger reconciliation of pending transfers:
   ```bash
   docker compose exec worker uv run --no-sync python -c "
   from app.tasks.nas_tasks import reconcile_nas_transfers
   print(reconcile_nas_transfers.delay().get(timeout=30))
   "
   ```

3. **Database Migrations (Alembic)**:
   ```bash
   DATABASE_URL=postgresql+psycopg://postgres:postgres@localhost:5436/data_automation \
   uv run --no-sync alembic current
   ```
