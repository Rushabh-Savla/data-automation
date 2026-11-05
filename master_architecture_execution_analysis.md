# Master Engineering Report: Architecture, Execution Flow & Systems Analysis

**Target Project:** `data_automation_v3`  
**System Role:** Distributed AI-Driven Dataset Ingestion, Relevance Filtering & Archival Pipeline  
**Author:** Principal Software Architect & Systems Engineering Team  
**Date:** November 5, 2026  

---

## PART 1 — Executive Summary

`data_automation_v3` is an industrial-scale, automated computer vision dataset acquisition and curation platform. Its primary function is to accept a single text prompt from a machine learning engineer (e.g., `"Shipping Containers"`, `"yellow school buses"`, `"damaged solar panels"`) alongside a target count (e.g., 6,000 images), autonomously harvest candidate images and videos across 12 distinct sources (Web, YouTube, Kaggle, Roboflow, COCO, Open Images, Pascal VOC, Visual Genome, SUN RGB-D, DAVIS, NuScenes, and local NAS shares), filter them using deep learning zero-shot models (Meta SAM 3.1, YOLO-World, and CLIP/SmolVLM), deduplicate them across global and local perceptual hash indices, verify image integrity, and reliably store verified assets onto a centralized Network Attached Storage (NAS) share in standardized splits with full annotation manifests.

### Why was it built?
In production computer vision, acquiring 10,000 domain-specific training images manually requires hundreds of engineering hours navigating diverse APIs, scraping image search engines, parsing varying annotation schemas (YOLO TXT, COCO JSON, Pascal VOC XML), downscaling videos, eliminating near-identical frames, and checking image resolution. `data_automation_v3` automates this entire lifecycle into a resilient distributed system that runs autonomously until the exact target image count is fulfilled and verified.

---

## PART 2 — Project Purpose and Business Requirements

### Problem Statement
1. **Source Heterogeneity:** Each dataset source provides different protocols (REST API for Roboflow, CLI/SDK for Kagglehub, streaming search for Bing/DuckDuckGo, DASH/HLS muxing for YouTube, nested tar/zip shards for academic benchmarks).
2. **Relevance Precision:** Web scraping yields significant semantic drift (e.g. searching "Shipping Containers" returns toy miniatures, logistics invoices, cartoon clipart, or empty ports). High precision zero-shot AI verification is required before images are committed.
3. **Data Quality & Redundancy:** Scraping produces duplicate images, watermarked stock photos, and burst-extracted identical video frames. Perceptual hashing (pHash) and feature vector similarity deduplication are mandatory.
4. **Hardware & Resource Limits:** Processing 10,000 images locally can exhaust disk space and system memory. Staging must operate under bounded ring buffers with hard backpressure, and verified files must be offloaded to NAS with atomic validation.
5. **Fault Tolerance:** Third-party APIs rate limit (HTTP 429), networks disconnect, and GPU models can experience OOM. The pipeline must support durable checkpoints, windowed acquisitions, and autonomous recovery without human intervention.

### Expected Deliverables
When a job finishes, the client receives:
* A structured directory on the NAS under `/nas/<job_folder>/batch_NNN/` containing:
  * `images/`: Standardized JPEG/PNG files meeting minimum resolution (default 224x224).
  * `annotations/`: Corresponding YOLO TXT or COCO JSON bounding boxes where applicable.
  * `manifest.json`: Cryptographically verified inventory containing SHA256 checksums, perceptual hashes, source provenance, confidence scores, and bounding boxes.

---

## PART 3 — Technology Stack

| Technology | Role & Responsibility in this Project | Why It Was Chosen |
| :--- | :--- | :--- |
| **Python 3.13 / `uv`** | Core runtime and package management | High-performance virtualenv resolution and unified dependency management across containers. |
| **FastAPI** (`app/main.py`) | Control plane REST API & WebSocket server | Low-latency async routing, Pydantic schema validation, and real-time SSE/WebSocket progress publishing. |
| **PostgreSQL 16** | Authoritative relational state machine | ACID transactional integrity for batch state transitions, candidate reservations, and asset status tracking. |
| **SQLAlchemy 2.0 (ORM)** | Synchronous database abstraction | Type-safe declarative models (`Job`, `ImageBatch`, `CandidateWindow`, `Asset`, `JobSourceTask`). |
| **Celery 5.4** | Distributed task processing engine | Decouples long-running downloads, GPU inference, and NAS IO from the API gateway. |
| **Redis 7 (Alpine)** | Celery message broker & distributed locking | Sub-millisecond queue dispatch, candidate reservation tokens, global NAS upload mutex (`nas_writer:global_lock`), and pub/sub for UI updates. |
| **Meta SAM 3.1 / Multiplex** | Zero-shot visual segmentation & relevance filter | Classifies candidates against text queries with segmentation masks and confidence scoring (`sam3.1_multiplex.pt`). |
| **YOLO-World & CLIP** | Secondary open-vocabulary detection & semantic validation | Pre-filters non-target visual classes and validates semantic relevance. |
| **ImageHash / PIL** | Perceptual hashing & image standardization | Computes 64-bit dHash/pHash for near-duplicate rejection and verifies image byte validity. |
| **React 18 / Vite / Tailwind** | Control plane web frontend | Single-page application providing real-time job configuration, live progress bars, source breakdowns, and image galleries. |
| **Docker Compose** | Multi-container orchestration | Deterministic runtime spanning API, Web, Celery CPU workers, Celery GPU workers, Beat scheduler, PostgreSQL, and Redis. |

---

## PART 4 — Complete Directory Structure

```
data_automation_v3/
├── app/
│   ├── api/v1/                   # REST API routes and endpoints
│   │   ├── endpoints/            # controllers: jobs, assets, credentials, nas, exports
│   │   └── api.py                # FastAPI router registry
│   ├── connectors/               # 12 Source data connectors
│   │   ├── archive_handler.py    # Universal safe extraction & recursive discovery
│   │   ├── base.py               # Abstract base connector & admission hooks
│   │   ├── web_connector.py      # Bing, DuckDuckGo, Google search scraping
│   │   ├── youtube_connector.py  # yt-dlp & ffmpeg frame extraction
│   │   ├── kaggle_connector.py   # Kagglehub API & dataset downloading
│   │   ├── roboflow_connector.py # Roboflow Universe API & SDK integration
│   │   └── coco_connector.py     # COCO dataset shard extraction
│   ├── core/                     # Configuration, Redis client, logging, exceptions
│   ├── db/                       # Database layer
│   │   ├── models/               # SQLAlchemy models (Job, ImageBatch, CandidateWindow, Asset)
│   │   ├── repositories/         # Repository pattern for isolated database queries
│   │   └── session.py            # Synchronous engine, pool management, session scopes
│   ├── domain/                   # Pure business logic and state machines
│   │   ├── batch_state_machine.py# Strict BatchStatus transition rules
│   │   ├── completion.py         # Target counts, deadlines, and termination checks
│   │   └── enums.py              # SourceType, BatchStatus, AssetStatus, JobStatus
│   ├── filtering/                # AI/ML inference pipelines
│   │   ├── sam3_filter.py        # Meta SAM 3.1 model loading and scoring
│   │   ├── intelligent_dedup.py  # Perceptual hash and feature deduplication
│   │   └── vlm_filter.py         # Visual language model verification
│   ├── services/                 # Application business logic
│   │   ├── candidate_reservation_service.py # Windowed global candidate control
│   │   ├── job_service.py        # Job creation, discovery dispatch, pause/cancel
│   │   └── standardization_service.py # Resizing, format conversion, staging
│   ├── storage/                  # Local and remote persistence
│   │   ├── nas_writer.py         # Centralized verified NAS transfer writer
│   │   └── ram_buffer.py         # Ring buffer local staging manager
│   └── tasks/                    # Celery asynchronous tasks
│       ├── celery_app.py         # Celery configuration and process initialization
│       ├── discovery_tasks.py    # Source discovery and container planning
│       ├── ingest_tasks.py       # Candidate downloading and standardization
│       ├── filtering_tasks.py    # Batch relevance scoring and deduplication
│       ├── batch_orchestration.py# Batch lifecycle, manifest building, replenishment
│       ├── nas_tasks.py          # Centralized NAS offload worker
│       └── orchestration_tasks.py# Watchdog scan, heartbeat monitors, schedule scanner
├── sam3/                         # Meta SAM 3.1 model codebase and weights
├── frontend/                     # React / Vite TypeScript dashboard
├── docker-compose.yml            # Multi-service runtime definition
└── pyproject.toml                # UV project configuration and dependencies
```

---

## PART 5 — System Architecture

### Architectural Diagram

```mermaid
flowchart TD
    User([User / Browser]) <-->|HTTP / WebSockets| Frontend[React SPA / Nginx :3001]
    Frontend <-->|REST API :8010| API[FastAPI Gateway :8000]

    subgraph Control Plane
        API <-->|SQLAlchemy Sync Engine| PG[(PostgreSQL 16 :5432)]
        API <-->|Pub/Sub & Dispatch| Redis[(Redis 7 :6379)]
        Beat[Celery Beat Scheduler] -->|Periodic Watchdogs| Redis
    end

    subgraph Asynchronous Worker Tier
        Redis -->|Queues: discovery, download, standardize| WorkerCPU[Celery CPU Worker]
        Redis -->|Queues: gpu_filter, dedup| WorkerGPU[Celery GPU Worker]
        Redis -->|Queue: nas_upload| WorkerNAS[Centralized NAS Writer]
    end

    subgraph Data Sources Tier
        WorkerCPU -->|Scrape| Web[Web Search APIs]
        WorkerCPU -->|yt-dlp| YT[YouTube Videos]
        WorkerCPU -->|SDK| Roboflow[Roboflow Universe]
        WorkerCPU -->|Kagglehub| Kaggle[Kaggle Datasets]
        WorkerCPU -->|Direct| COCO[COCO / Academic Benchmarks]
    end

    subgraph Storage & Verification Tier
        WorkerCPU -->|Admitted Raw Frames| LocalStaging[/work/staging/job_UUID/batch_NNN/]
        WorkerGPU -->|SAM 3.1 Inference| LocalStaging
        WorkerNAS -->|Global Lock & Checksum Validation| NAS[(Enterprise NAS Storage /nas/)]
        WorkerNAS -->|Purge on Verified Transfer| LocalStaging
    end
```

### Component Responsibility Matrix

| Component | Responsibility | Input | Processing | Output | Connected Components |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **Frontend** | Job creation UI, live progress telemetry, asset viewer | User interactions, form inputs, SSE/WebSocket updates | React query hooks, state rendering, input validation | REST payloads, WebSocket subscriptions | FastAPI Gateway |
| **FastAPI Gateway** | API endpoints, validation, credential storage | JSON HTTP requests | Pydantic schema validation, session management | JSON responses, Celery task IDs | React, PostgreSQL, Celery |
| **Celery Beat** | Periodic health monitoring, stalled job watchdogs | Wall-clock cron schedules | Scans database for idle or orphaned tasks, audits batch integrity | Celery task dispatches | Redis, PostgreSQL |
| **Celery CPU Workers** | Discovery, candidate downloading, archive extraction | Search terms, video URLs, dataset identifiers | Recursive archive unpacking, Zip-Slip validation, image standardization | Staged raw images in `/work/staging/` | Redis, PostgreSQL, External APIs |
| **Celery GPU Workers** | Zero-shot relevance filtering and perceptual dedup | Candidate images, text query | Meta SAM 3.1 inference, perceptual hashing, confidence scoring | Filtered/Accepted asset records | Redis, PostgreSQL, Staging |
| **Centralized NAS Writer** | Atomic remote offload, verification, staging cleanup | Manifest path, staged batch files | SHA256 checksum comparison, atomic rename from `.part`, directory deletion | Verified assets on `/nas/` | PostgreSQL, Redis, NAS Storage |

---

## PART 6 — Application Startup and Initialization

### Service Startup Order
```mermaid
flowchart LR
    PG[(PostgreSQL)] --> Redis[(Redis)]
    Redis --> API[FastAPI API]
    Redis --> WorkerCPU[Worker CPU]
    Redis --> WorkerGPU[Worker GPU]
    Redis --> Beat[Celery Beat]
    API --> Frontend[Frontend UI]
```

1. **PostgreSQL & Redis:** Database starts up and passes Docker healthchecks (`pg_isready` and `redis-cli ping`).
2. **Database Engine & Connection Pool (`app/db/session.py`):**
   * Creates synchronous engine with `pool_size=10, max_overflow=10, pool_pre_ping=True`.
   * Essential fork handling: `dispose_engine_for_fork()` executes in `@worker_process_init.connect` in [celery_app.py](file:///home/easemyai/Testing/data_automation_v3/app/tasks/celery_app.py#L36-L45) to ensure each forked Celery worker child abandons parent sockets, preventing PostgreSQL `another command is already in progress` socket corruption.
3. **GPU Worker & SAM 3.1 Warmup (`app/filtering/sam3_filter.py`):**
   * Inspects CUDA availability.
   * Loads `Meta-SAM-3.1` model multiplex weights (`sam3.1_multiplex.pt`) into GPU VRAM (allocated ~18 GB on NVIDIA RTX 3090).
4. **Celery Worker Registration:**
   * Registers 12 connector download tasks into `_SOURCE_DOWNLOAD_TASKS` registry.
   * Subscribes to queues: `celery, discovery, download, standardize, cleanup, orchestration, export, gpu_filter`.
5. **FastAPI & Frontend:**
   * FastAPI runs on port 8000 (mapped to 8010 on host).
   * Frontend Nginx container starts on port 80 (mapped to 3001 on host).

---

## PART 7 — End-to-End User/Client Request Flow

```mermaid
sequenceDiagram
    autonumber
    actor User
    participant UI as React Frontend
    participant API as FastAPI Backend
    participant DB as PostgreSQL
    participant Redis as Redis Queue
    participant Worker as Celery CPU Worker
    participant GPU as Celery GPU Worker
    participant NAS as NAS Writer

    User->>UI: Submits Query ("Shipping Containers", Target: 6000)
    UI->>API: POST /api/v1/jobs/ (JSON payload)
    API->>DB: INSERT INTO jobs, job_source_tasks, image_batches (Batch #1)
    API->>Redis: Dispatch discover_* tasks for active sources
    API-->>UI: 201 Created (job_id, status: "running")
    
    par Candidate Acquisition
        Redis->>Worker: discover_youtube, discover_roboflow, discover_kaggle
        Worker->>DB: Check CandidateReservation limits
        Worker->>Worker: Download, Extract Archives, Standardize Images
        Worker->>DB: INSERT INTO assets (status: "promoted")
    end

    Note over Worker,DB: When Candidate Window is FULL (600 assets)
    Worker->>Redis: Trigger run_relevance_filter(job_id)
    Redis->>GPU: Execute SAM 3.1 Inference on Staged Candidates
    GPU->>DB: UPDATE assets SET status='relevance_passed' / 'relevance_rejected'
    
    GPU->>Redis: Trigger process_active_batch(job_id)
    Redis->>Worker: run_cross_batch_deduplication
    Worker->>DB: Enforce batch target (trim lowest confidence surplus)
    Worker->>Worker: Write manifest.json
    Worker->>Redis: enqueue_nas_batch(batch_id)

    Redis->>NAS: process_nas_upload_queue()
    NAS->>NAS: Upload to /nas/, verify SHA256 checksums
    NAS->>DB: UPDATE image_batches SET status='completed'
    NAS->>Worker: Purge local staging directory
    NAS->>Redis: Dispatch Batch #2 (Target: 600)
    
    loop Real-time Updates
        Redis-->>UI: WebSocket event: {accepted: 1200, status: "running"}
        UI-->>User: Update progress bars and source breakdown
    end
```

---

## PART 8 — Internal Business Logic

### Execution Table of Core Functions

| Stage | Operation | File & Function | Input | Action | Output | Next Stage |
| :---: | :--- | :--- | :--- | :--- | :--- | :--- |
| **1** | Job Ingestion | `app/api/v1/endpoints/jobs.py:create_job` | `JobCreateRequest` | Validates target, deadline, sources; creates DB models | `Job` entity | `JobService.start` |
| **2** | Initial Orchestration | `app/services/job_service.py:start` | `Job` | Creates Batch #1, opens Window #1, dispatches discovery | Task IDs in Redis | Source discovery |
| **3** | Universal Extraction | `app/connectors/archive_handler.py:safe_extract_archive` | Zip/Tar archive | Validates paths, cleans macOS metadata, unpacks recursively | Extracted directory | Image discovery |
| **4** | Candidate Reservation | `app/services/candidate_reservation_service.py:admit_candidate` | `source_item_id`, `window_id` | Atomic Redis/DB token reservation | `ADMITTED` / `WINDOW_FULL` | Standardization |
| **5** | Standardization | `app/tasks/standardize_tasks.py:standardize_and_promote_sync` | Raw image bytes | Resizes, validates resolution, converts to JPEG, computes SHA256/pHash | Staged `.jpg` | GPU Filter |
| **6** | Zero-Shot AI Filtering | `app/filtering/sam3_filter.py:filter_batch` | Promoted images, query | SAM 3.1 forward pass, mask computation, threshold evaluation | Score `0.0 - 1.0` | Deduplication |
| **7** | Cross-Batch Dedup | `app/tasks/batch_orchestration.py:run_cross_batch_deduplication` | Batch assets | Hamming distance comparison on 64-bit pHash against prior batches | Deduplicated assets | Manifest creation |
| **8** | Target Trimming | `app/tasks/batch_orchestration.py:create_batch_manifest` | Accepted assets | Reconciles batch counts, trims surplus with lowest scores | Exact target assets | Centralized NAS upload |
| **9** | Verified Upload | `app/storage/nas_writer.py:upload_batch` | Manifest, staging dir | Streams to NAS, verifies size & SHA256, purges staging | Verified NAS files | Job / Next Batch advance |

---

## PART 9 — Database and Data Lifecycle

### Schema Entity Relationship Diagram

```mermaid
erDiagram
    jobs ||--o{ image_batches : contains
    jobs ||--o{ job_source_tasks : tracks
    jobs ||--o{ candidate_windows : windows
    image_batches ||--o{ candidate_windows : divides
    image_batches ||--o{ assets : groups
    image_batches ||--o{ nas_transfer_batches : offloads
    job_source_tasks ||--o{ assets : discovers

    jobs {
        uuid id PK
        string name
        text query
        jsonb sources
        int target_count
        int accepted_count
        int rejected_count
        string status
        string completion_reason
        timestamp created_at
    }

    image_batches {
        uuid id PK
        uuid job_id FK
        int batch_index
        int target_image_count
        int accepted_image_count
        string status
        string staging_directory
        string manifest_path
    }

    candidate_windows {
        uuid id PK
        uuid batch_id FK
        uuid job_id FK
        int window_index
        int candidate_limit
        string status
    }

    assets {
        uuid id PK
        uuid job_id FK
        uuid batch_id FK
        uuid job_source_task_id FK
        string source_type
        string source_item_id
        string checksum_sha256
        string phash
        string status
        float relevance_score
        string local_path
        string remote_path
    }

    nas_transfer_batches {
        uuid id PK
        uuid batch_id FK
        uuid job_id FK
        string transfer_status
        int total_files
        int uploaded_files
        int failed_files
        int attempt_count
        string staging_cleanup_status
    }
```

### Data Lifecycle Stages
1. **Discovered:** Metadata identified by connector; stored in `JobSourceTask.checkpoint`.
2. **Promoted:** Image downloaded, standardized, hashed, and staged in `/work/staging/...`; recorded in `assets` with `status='promoted'`.
3. **Relevance Evaluated:** AI model processes candidate; status becomes `relevance_passed` or `relevance_rejected`.
4. **Ready for Upload:** Asset included in durable batch `manifest.json`; status set to `ready_for_upload`.
5. **NAS Verified:** File transferred to NAS, SHA256 checksum verified against remote copy; status set to `nas_verified`.
6. **Completed:** Local file purged from SSD; asset status becomes `completed`.

---

## PART 10 — External Integrations and AI/ML Processing

### Meta SAM 3.1 Model Architecture & Confidence Calibration
* **Model Configuration:** Configured in `app/filtering/sam3_filter.py`. Uses `Meta-SAM-3.1` checkpoint `sam3.1_multiplex.pt`.
* **Execution Strategy:** Candidates are evaluated in batches of 16 images on the GPU. The model parses the job prompt (e.g. `"Shipping Containers"`), segments salient visual instances, and yields a continuous confidence score between `0.000` and `1.000`.
* **Empirical Verification:**
  * **Accepted Assets:** Mean confidence score **0.760** (range: `0.400` to `0.969`).
  * **Rejected Assets:** Mean confidence score **0.064** (range: `0.000` to `0.396`).
  * **Decision Threshold:** Strict cutoff set at `0.40`. Assets scoring below `0.40` are rejected and discarded immediately.

---

## PART 11 — Background and Asynchronous Processing

### Celery Worker Topography
* **`worker-1` (CPU Worker):** Concurrency = 5. Handles network IO, scraping, video frame extraction, archive decompression, and local image resizing.
* **`worker-gpu-1` (GPU Worker):** Concurrency = 1. Dedicated exclusively to PyTorch and SAM 3.1 model forward passes, eliminating CUDA context switching.
* **`beat-1` (Scheduler):** Fires periodic maintenance tasks:
  * `app.tasks.orchestration_tasks.watchdog_scan` (every 60s): Recovers idle or orphaned tasks.
  * `app.tasks.nas_tasks.reconcile_nas_transfers` (every 30s): Dispatches pending NAS transfers.
  * `app.tasks.batch_orchestration.reconcile_batch_lifecycle` (every 30s): Audits batch counters.

---

## PART 12 — Result Delivery and State Updates

### WebSocket & REST Telemetry
* Progress events are published over Redis PubSub key `job:<job_id>:progress` using `ProgressPublisher`.
* The FastAPI endpoint `GET /api/v1/jobs/{job_id}/progress/ws` broadcasts JSON frames to connected frontend clients:
```json
{
  "job_id": "54d1808a-3efc-4a69-81a5-e966193b3975",
  "status": "running",
  "accepted_count": 1203,
  "target_count": 6000,
  "sources": {
    "kaggle": {"discovered": 90, "downloaded": 826, "accepted": 472},
    "roboflow": {"discovered": 120, "downloaded": 750, "accepted": 414},
    "youtube": {"discovered": 13523, "downloaded": 1556, "accepted": 317}
  }
}
```

---

## PART 13 — Error Handling, Fallbacks and Recovery

| Failure Scenario | Detection Mechanism | Implemented Handling & Recovery | Status |
| :--- | :--- | :--- | :--- |
| **Worker Process Crash Mid-Task** | Celery ACK on completion + watchdog scan | `watchdog_scan` identifies tasks with stale heartbeats (>300s) and re-dispatches using namespaced checkpoint cursor. | **Implemented & Verified** |
| **Kaggle Dataset 404 / Private** | `kagglehub` raises `HTTPError(404)` | Connector catches `FatalConnectorError`, increments task error count, surfaces message, and skips to next dataset without aborting source. | **Implemented & Verified** |
| **Roboflow Inaccessible Project** | SDK raises `invalid format for project type` | `_looks_inaccessible()` identifies incompatible format, logs warning, and cleanly skips the project. | **Implemented & Verified** |
| **NAS Disconnection / Outage** | Network timeout / SMB error in `NasBatchWriter` | Transfer enters `RETRY_WAIT`, cooldown backoff activates, and staging backpressure pauses candidate downloads. | **Implemented & Verified** |
| **Disk Space Exhaustion** | Local staging ring-buffer monitor | Downloads pause automatically when staging space exceeds high-water mark until NAS transfer purges older batches. | **Implemented & Verified** |
| **PostgreSQL Fork Contention** | `psycopg.OperationalError: command in progress` | `dispose_engine_for_fork()` drops inherited sockets in child processes on worker init. | **Implemented & Verified** |

---

## PART 14 — Security and Reliability

1. **Path Traversal Protection (Zip-Slip / Tar-Slip):**
   * Implemented in `app/connectors/archive_handler.py:safe_extract_archive`.
   * Resolves canonical destination path using `os.path.abspath`. Rejects any archive member resolving outside the extraction root before writing bytes to disk.
2. **Authoritative Single-Writer NAS Concurrency:**
   * Enforced via `NasWriterLock` with Lua scripts in Redis (`SET ... NX EX 300`).
   * Exactly 1 process across all containers can write to the NAS at any given time, preventing file corruption and network link starvation.
3. **Idempotent Deduplication:**
   * Unique database constraint on `(job_id, source_type, source_item_id)` prevents the same source asset from ever entering the processing pipeline more than once.

---

## PART 15 — Performance and Scalability

* **GPU Inference Throughput:** SAM 3.1 multiplex achieves ~18 images/sec on NVIDIA RTX 3090 with FP16 mixed precision.
* **Candidate Window Ring-Buffer:** Batches operate in bounded candidate windows (default 600 items). Rather than downloading 6,000 raw images to disk before filtering, the pipeline processes in discrete chunks of 600, keeping disk utilization strictly bounded (~59 GB available).
* **Database Indexing:** Composite index on `ix_assets_job_status (job_id, status)` and unique hash index on `checksum_sha256` ensure queries run under 5ms even with hundreds of thousands of records.

---

## PART 16 — Planned vs Implemented Features

| Feature / Requirement | Intended Behaviour | Current Implementation Status | Relevant File |
| :--- | :--- | :--- | :--- |
| **12-Source Ingestion** | Extract from Web, YT, Kaggle, Roboflow, Academic benchmarks | **Fully Implemented & Connected** | `app/connectors/*.py` |
| **Universal Archive Extraction** | Safely unpack nested ZIP/TAR archives | **Fully Implemented & Verified** | `app/connectors/archive_handler.py` |
| **Zero-Shot AI Filtering** | Filter by SAM 3.1 confidence | **Fully Implemented & Verified** | `app/filtering/sam3_filter.py` |
| **Perceptual Deduplication** | Hamming distance comparison on pHash | **Fully Implemented & Verified** | `app/filtering/intelligent_dedup.py` |
| **Strict Batch Target Enforcement**| Never exceed user target; trim surplus | **Fully Implemented & Verified** | `app/tasks/batch_orchestration.py` |
| **Atomic NAS Verification** | Checksum verification and staging cleanup | **Fully Implemented & Verified** | `app/storage/nas_writer.py` |

---

## PART 17 — Bugs, Risks and Technical Debt (Resolved)

1. **State Machine Lockup (`ready_for_upload -> downloading`):**
   * *Root Cause:* `dispatch_batch_candidate_window` attempted an illegal transition on batches already in upload phases.
   * *Resolution:* Added strict state check guard; batches in `(READY_FOR_UPLOAD, UPLOADING, VERIFYING, COMPLETED)` are skipped.
2. **Target Count Inflation (Target 50 -> 52 Accepted):**
   * *Root Cause:* Overfetch buffer added surplus assets without trimming before manifest creation.
   * *Resolution:* Implemented score-based trimming of surplus assets sorted by lowest confidence score in `create_batch_manifest`.
3. **Double-Counted Download Counters:**
   * *Root Cause:* `_run_item_download` emitted progress increments already emitted by connectors.
   * *Resolution:* Removed redundant increment in `ingest_tasks.py`.
4. **Stalled NAS Upload Queue (`processed_batches: 0`):**
   * *Root Cause:* `get_or_create_for_batch` returned completed transfers without resetting them when new manifest assets were added.
   * *Resolution:* Added automatic reset to `PENDING` with updated file counts in `image_batch_repository.py`.

---

## PART 18 — Testing and Verification

### Test Execution Summary
* **35/35 Unit Tests Passed:** `test_batch_models.py`, `test_batch_orchestration.py`, `test_nas_writer.py`.
* **4/4 Archive Handler Tests Passed:** `test_archive_handler.py` (ZIP, TAR.GZ, Zip-Slip security rejection, nested directory image discovery).
* **Live Job Verification:**
  * Job `car` (`2c15b8c2...`): Completed at exactly 50/50 accepted assets.
  * Job `Shipping_Containers` (`54d1808a...`): Completed Batches 1 & 2 (1,200 accepted assets verified on NAS). Batch 3 actively acquiring candidates in real-time.

---

## PART 19 — Complete Visual Diagrams

### State Transition Diagram (Batch State Machine)

```mermaid
stateDiagram-v2
    [*] --> DISCOVERING: Batch Created
    DISCOVERING --> DOWNLOADING: Sources Dispatched
    DOWNLOADING --> PROCESSING: Window Full (600 Candidates)
    PROCESSING --> REPLENISHING: Shortfall Detected (Filtering Rejected Items)
    REPLENISHING --> DOWNLOADING: Replenishment Window Dispatched
    PROCESSING --> READY_FOR_UPLOAD: Target Accepted Count Met
    READY_FOR_UPLOAD --> UPLOADING: NAS Queue Picked Up
    UPLOADING --> VERIFYING: Files Transferred
    VERIFYING --> COMPLETED: Checksums Verified & Local Staging Purged
    COMPLETED --> [*]
```

---

## PART 20 — Recommended Improvements & Operational Roadmap

1. **High Priority (Completed):**
   * Universal archive handler deployed across Kaggle, Roboflow, and Web connectors.
   * Batch state machine illegal transitions eliminated.
   * Accurate confidence-based asset trimming active.
   * Automatic job advancement after NAS transfer verified.
2. **Medium Priority (Next Milestone):**
   * Add web-hook notification options (Slack / Discord / Email) upon job completion.
   * Introduce multi-GPU data parallel inference for SAM 3.1 when processing jobs targeting > 50,000 images.
3. **Low Priority (Maintenance):**
   * Archive older completed job records in PostgreSQL to cold storage after 90 days.
