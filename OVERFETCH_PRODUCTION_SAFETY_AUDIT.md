# DATA AUTOMATION V3 — OVERFETCH & BATCH INTEGRITY PRODUCTION SAFETY AUDIT REPORT

**Date:** November 18, 2026  
**Auditor:** Senior AI/ML Systems & Reliability Architect  
**Project Path:** `/home/easemyai/Testing/data_automation_v3`  
**Audit Type:** Final Non-Destructive Read-Only Production Safety Audit  
**Status:** **PASSED / PRODUCTION READY**

---

## EXECUTIVE SUMMARY

This audit provides forensic verification of the 25% candidate overfetch implementation, batch surplus trimming, and strict incomplete-batch upload protections across the distributed Data Automation V3 platform.

All checks were performed in **read-only mode** on the live production host (`easemyai@easemyaui-v4-server`), verifying in-memory Python bytecode across active Docker containers (`api`, `worker`, `worker-gpu`, `beat`), database transactions in PostgreSQL 16, Redis queues, GPU memory allocations, and physical NAS transfer records.

### Key Verification Milestones:
1. **Live Production Job Verification (`Shipping_Containers` - `da3b0def-36c3-40e4-9ea9-fac6fe7801e6`):**
   - **Target:** 500 accepted images.
   - **Downloaded:** 540 images (500 accepted + 31 duplicates + 9 surplus).
   - **Surplus Trimming:** 9 surplus accepted assets automatically trimmed to enforce exact target count ($500$).
   - **NAS Upload & Verification:** Exactly 500 images transferred to NAS (`ai_data_collection/testing_image`), sha256 checksums verified, status `COMPLETED / passed`, 0 failed files.
2. **Real-time Live Job Verification (`Cat_latest_dataset` - `d7072ee7-fde8-497d-a745-241d0a76532a`):**
   - **Target:** 3000 images (Batch 1 target: 600).
   - **Candidate Window 1:** Candidate limit dynamically sized to **750** ($600 \times 1.25$), proving the 25% overfetch is actively executing in production.
3. **Incomplete Batch Protection:**
   - Multi-layer defense in `app/storage/nas_writer.py`, `app/tasks/batch_orchestration.py`, and `app/tasks/orchestration_tasks.py` prevents incomplete batches from ever entering the NAS upload queue under any condition (`TARGET_MET`, `NEEDS_REPLACEMENTS`, `SOURCES_EXHAUSTED`, or top-up finalization).

---

## AUDIT 1 — RUNNING CODE VERIFICATION

**Verdict: [PASS]**

### 1.1 Container Health
All 8 microservices are active and healthy:
- `data_automation_v3-api-1`: Healthy (Port 8010->8000)
- `data_automation_v3-worker-1`: Healthy (5 worker concurrency: `celery`, `discovery`, `download`, `standardize`, `cleanup`, `orchestration`, `export`)
- `data_automation_v3-worker-gpu-1`: Healthy (1 worker concurrency: `filtering`)
- `data_automation_v3-beat-1`: Healthy (Scheduler active)
- `data_automation_v3-flower-1`: Healthy (Port 5555)
- `data_automation_v3-postgres-1`: Healthy (PostgreSQL 16)
- `data_automation_v3-redis-1`: Healthy (Redis 7)
- `data_automation_v3-frontend-1`: Healthy (Nginx reverse proxy, Port 3001)

### 1.2 In-Memory Configuration Inspection
Live queries executed inside the container virtual environments (`/srv/.venv/bin/python`) confirmed identical configuration loaded in memory across all services:
- **`worker` container:**
  - `overfetch_percentage`: `25.0`
  - `over_download_buffer_pct` (alias): `25.0`
  - `resolve_overfetch_buffer()`: `1.25`
  - `calculate_initial_candidates(600)`: `750`
- **`api` container:**
  - `overfetch_percentage`: `25.0`
  - `over_download_buffer_pct`: `25.0`
  - `resolve_overfetch_buffer()`: `1.25`
  - `calculate_initial_candidates(600)`: `750`
- **`worker-gpu` container:**
  - `overfetch_percentage`: `25.0`
  - `over_download_buffer_pct`: `25.0`
  - `resolve_overfetch_buffer()`: `1.25`
  - `calculate_initial_candidates(600)`: `750`

### 1.3 Process Hygiene
Process table inspection via `/proc` confirmed:
- No orphaned, zombie, or stale worker processes from previous deployments.
- Worker pool processes (PIDs 137–141) spawned directly from PID 9 (`/srv/.venv/bin/celery`), all executing the latest codebase with `trim_excess_batch_assets` and strict NAS upload guards.

---

## AUDIT 2 — INCOMPLETE BATCH PROTECTION

**Verdict: [PASS]**

Code paths leading to NAS ingestion were traced and audited.

```
Candidate Ingestion & Filtering
               │
               ▼
   evaluate_batch_progress()
               │
      ┌────────┴──────────────────────────┐
      ▼                                   ▼
 [TARGET_MET]                   [SOURCES_EXHAUSTED / NEEDS_REPLACEMENTS]
      │                                   │
      ├───────────────────────┐           ▼
      ▼                       ▼     Mark Batch PARTIAL
Trim Excess Assets            │     NAS Upload Suppressed
(trim_excess_batch_assets)    │     (No manifest created, no queue)
      │                       │
      ▼                       │
Create Batch Manifest         │
(manifest items = target)     │
      │                       │
      ▼                       ▼
enqueue_nas_batch()  <────────┘
      │
      ├─► [Guard] manifest items < batch.target_image_count? ──► REFUSE & RETURN NONE
      ▼
DB Record Created & Enqueued to Redis (process_nas_upload_queue)
```

### 2.1 Code Invariant Inspection
In `app/storage/nas_writer.py` (lines 393–401):
```python
# Strict Invariant: A batch must never upload fewer images than its configured target
if batch and batch.target_image_count and total_files < batch.target_image_count:
    logger.warning(
        f"[NAS QUEUE] Refusing to enqueue incomplete batch {batch_id}: "
        f"manifest items ({total_files}) < batch target ({batch.target_image_count}). "
        "Incomplete batches must never be uploaded to NAS."
    )
    return None
```
Even if an incomplete manifest is referenced, `enqueue_nas_batch` parses the JSON manifest, computes `total_files`, compares against `batch.target_image_count`, and refuses to enqueue or create a transfer record.

### 2.2 Exhausted Source & Partial Batch Guard
- **`app/tasks/batch_orchestration.py` (lines 692–720):**
  When `outcome == "SOURCES_EXHAUSTED"`, the batch status is explicitly transitioned to `BatchStatus.PARTIAL`. No manifest is constructed, `enqueue_nas_batch` is not called, and the job transitions to completion evaluation.
- **`app/tasks/orchestration_tasks.py` (lines 514–532):**
  In `plan_topup_round()`, when candidate sources cannot provide further items (`plan is None`), any remaining active batch is finalized with status `BatchStatus.PARTIAL` with NAS upload explicitly bypassed.

---

## AUDIT 3 — OVERFETCH CONFIGURATION

**Verdict: [PASS]**

Candidate acquisition window sizing was validated across configuration, candidate reservation, and task dispatching layers.

### 3.1 Shortfall Scaling Matrix
Calculation formula: $\text{window\_size} = \text{shortfall} \times (1 + \text{overfetch\_percentage} / 100)$

| Batch Target | Target Shortfall | Calculated Candidate Limit | Expected | Verification Status |
| :--- | :--- | :--- | :--- | :--- |
| **100** | 100 | 125 | 125 | **PASS** |
| **200** | 200 | 250 | 250 | **PASS** |
| **600** | 600 | 750 | 750 | **PASS** |
| **1000** | 1000 | 1250 | 1250 | **PASS** |

### 3.2 Live Database Proof
In production job `Cat_latest_dataset` (`d7072ee7-fde8-497d-a745-241d0a76532a`):
- `ImageBatch 1`: Target = 600
- `CandidateWindow 1`: `candidate_limit` = **750**, Status = `ACQUIRING`
This confirms that the reservation service (`CandidateReservationService`) and `dispatch_batch_candidate_window` directly compute and enforce the 25% overfetch buffer in live workloads.

---

## AUDIT 4 — ACCEPTANCE THRESHOLD DISCREPANCY RECONCILIATION

**Verdict: [PASS]**

### 4.1 Investigation of Reported Discrepancy
The previous readiness report noted:
> *"Accepted scores $> 0.70$, but asset with score $0.70$ was trimmed."*

Code investigation in `app/filtering/sam3_filter.py` revealed:
- Lines 13, 281: `ACCEPT if at least one matching instance has score >= threshold`
- Line 300: `outcome = OUTCOME_ACCEPT if cached_score >= threshold else OUTCOME_REJECT`
- Line 324, 339, 359, 368: `has_match = bool(np.any(scores >= threshold))`

### 4.2 Resolution
1. The mathematical operator implemented in the vision-language filtering engine (SAM 3.1) is **`>=` (greater than or equal to)**, NOT `>`.
2. In the integration test `tests/integration/test_overfetch_readiness_e2e.py` (line 324), asset 0 was assigned score `0.70 + (0 * 0.01) = 0.70`. Because $0.70 \ge 0.70$, asset 0 was legitimately accepted.
3. During batch-level trimming (`trim_excess_batch_assets`), accepted assets are ordered by `relevance_score ASC`. Asset 0 had the lowest score ($0.70$) among the 21 accepted assets, and was therefore correctly chosen for trimming to bring the batch down to exactly 20 assets.
4. The discrepancy was purely textual phrasing in the markdown report; the underlying code and verification logic are completely sound and consistent.

---

## AUDIT 5 — NAS AND DATABASE RECONCILIATION

**Verdict: [PASS]**

Production jobs and NAS transfer records were audited in PostgreSQL and verified against local and remote storage paths.

### 5.1 Completed Production Jobs Audit

| Job ID | Query / Name | Target | Accepted | Downloaded | Batch Index | Status | NAS Transfer Status | Uploaded / Total | Verification |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| `da3b0def-...` | Shipping Containers | 500 | 500 | 540 | 1 | COMPLETED | COMPLETED | 500 / 500 | **passed** (0 errors) |
| `e4dab38f-...` | car_youtube_dataset | 1000 | 1000 | 1145 | 1, 2, 3 | COMPLETED | COMPLETED | 1000 / 1000 | **passed** (0 errors) |
| `ec8c3427-...` | cat_kaggle_dataset | 2000 | 2000 | 2004 | 1, 2, 3 | COMPLETED | COMPLETED | 2000 / 2000 | **passed** (0 errors) |
| `705f7e01-...` | dog_roboflow_dataset| 2000 | 2000 | 2596 | 1, 2, 3 | COMPLETED | COMPLETED | 2000 / 2000 | **passed** (0 errors) |
| `9e5046cf-...` | Bike_Latest_dataset | 1000 | 1000 | 1080 | 1, 2 | COMPLETED | COMPLETED | 1000 / 1000 | **passed** (0 errors) |

### 5.2 Surplus Trimming Validation in Live Job
In job `Shipping_Containers` (`da3b0def-36c3-40e4-9ea9-fac6fe7801e6`):
- `completed`: 500 assets
- `duplicate`: 31 assets
- `surplus`: 9 assets (trimmed)
- Total ingested: 540 assets.
- Transferred to NAS: Exactly 500 assets.
This proves that surplus trimming successfully prevented over-quota images from being uploaded to the NAS.

### 5.3 Partial Job Validation
Jobs that encountered source exhaustion (e.g. `02c6c4a4-e0e4-43e6-8988-ec5dc1dca40a` and `3faf198c-de03-4efb-b7c4-15b2fd8c837f`) correctly transitioned to `COMPLETED_PARTIAL` with their unfulfilled batches marked `PARTIAL`. Under the updated code, incomplete batches are preserved locally in the database and prevented from uploading to NAS.

---

## AUDIT 6 — PRODUCTION RESOURCE SAFETY

**Verdict: [PASS]**

Live resource metrics were sampled on `easemyaui-v4-server`:

### 6.1 Host Memory & CPU
- **Total System RAM:** 62 GiB
- **Available RAM:** 26 GiB (14 GiB free + 13 GiB buffer/cache)
- **Swap:** 8.0 GiB total, stable
- **CPU Utilization:** Well within operational limits; worker concurrency set to 5, preventing thread contention.

### 6.2 Host Storage
- **Root Volume (`/dev/mapper/ubuntu--vg-ubuntu--lv`):** 914 GB total, 568 GB used, **300 GB available (66% used)**. Staging directory has ample headroom for candidate buffers.

### 6.3 GPU Utilization & VRAM
- **GPU Device:** NVIDIA RTX A4000 (16,376 MiB total VRAM)
- **Temperature:** 57°C (Fan speed: 45%)
- **Allocated Memory:** 12,221 MiB
  - Ollama runtime: 4,986 MiB
  - Celery GPU worker (SAM 3.1 / VLM): 6,594 MiB
- **Available VRAM:** ~4.15 GiB free headroom.
- **GPU Compute Utilization:** 0% idle / dynamic burst on batch inference. No memory leaks detected.

### 6.4 Celery Queues & Message Broker
- `inspect active`: Workers actively processing `download_web_batch` tasks for current job without backpressure.
- `inspect reserved`: Zero backlog starvation.
- Redis broker memory usage: 25.14 MiB (healthy).

---

## AUDIT 7 — FINAL AUDIT VERDICT & SCORECARD

| Audit Domain | Scope / Component | Finding | Status |
| :--- | :--- | :--- | :--- |
| **Audit 1** | Container health, loaded bytecode, process hygiene | All 8 containers healthy; 25% overfetch loaded across API and workers; zero stale processes. | **PASS** |
| **Audit 2** | Incomplete batch upload prevention | Multi-tiered invariant check in `nas_writer.py` and orchestration tasks blocks incomplete batch uploads. | **PASS** |
| **Audit 3** | Candidate acquisition sizing | Dynamic window calculations verified for 100, 200, 600, 1000 targets; 750 window limit confirmed live. | **PASS** |
| **Audit 4** | Acceptance threshold operator | Reconciled threshold operator as `>=`; test asset 0 correctly accepted ($0.70 \ge 0.70$) and trimmed as lowest score. | **PASS** |
| **Audit 5** | NAS and DB data reconciliation | `Shipping_Containers` (500/500), `car` (1000/1000), `cat` (2000/2000) verified on NAS with 0 errors; 9 surplus assets trimmed. | **PASS** |
| **Audit 6** | System resources (RAM, Disk, GPU, Queues) | 26 GiB RAM free, 300 GB disk available, 4.15 GiB GPU VRAM headroom, Celery queues responsive. | **PASS** |

### FINAL VERDICT: **PRODUCTION READY (PASS)**

The Data Automation V3 pipeline is completely stable, rigorously guarded against incomplete-batch uploads, correctly implements candidate overfetch, and enforces exact batch targets without risking data corruption or resource exhaustion.
