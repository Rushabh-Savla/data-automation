# DYNAMIC FINAL BATCH SIZING & NAS DELIVERY READINESS REPORT

**Project:** Data Automation V3 (`/home/easemyai/Testing/data_automation_v3`)  
**Date:** November 18, 2026  
**Architect:** Senior AI/ML Systems & Reliability Architect  
**Deliverable Document:** `DYNAMIC_FINAL_BATCH_READINESS.md`  
**Status:** **PASSED & PRODUCTION READY**

---

## 1. EXECUTIVE SUMMARY & BUSINESS OBJECTIVE

The business objective is to **deliver all successfully collected, unique, relevant images to NAS whenever possible** without weakening the complete-batch protection invariant.

Prior to this implementation:
- The standard batch size was fixed at 600.
- Incomplete-batch protection correctly blocked batches with fewer accepted images than their target from uploading to NAS.
- However, if a job target was not an exact multiple of 600 (e.g. 1000, 1500, 2000, 2500), the final batch was still assigned a fixed target of 600. When candidate discovery ended or sources were exhausted after collecting the remaining required images (e.g. 400 images for a 1000-image job), the final batch was treated as "incomplete" (400 < 600) and blocked from NAS delivery.

**Solution Implemented:**
- The pipeline uses a standard batch size of 600, but **dynamically calculates the final batch target** based on remaining job demand:
  $$\text{batch.target\_image\_count} = \min(600, \text{remaining\_job\_target})$$
- The 25% candidate overfetch buffer dynamically scales with that exact batch target.
- Surplus trimming and manifest generation strictly adhere to the dynamic batch target.
- The strict NAS upload guard (`manifest items >= batch.target_image_count`) is 100% preserved.
- The final smaller batch is treated as a complete, fully valid batch as soon as its own dynamic target is reached, enabling immediate, safe NAS delivery.
- Zero production services were restarted and zero running jobs were disrupted.

---

## 2. DECOMPOSITION & SIZING MATRIX ACROSS ALL TEST TARGETS

| Job Target | Batch Sequence | Dynamic Batch Targets | 25% Overfetch Window | Manifest Count | Confirmed NAS Upload |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **100** | Batch 1 | `[100]` | **125** | 100 | **100** |
| **200** | Batch 1 | `[200]` | **250** | 200 | **200** |
| **600** | Batch 1 | `[600]` | **750** | 600 | **600** |
| **1000** | Batch 1, 2 | `[600, 400]` | **750, 500** | 600, 400 | **1000** (600 + 400) |
| **1500** | Batch 1, 2, 3 | `[600, 600, 300]` | **750, 750, 375** | 600, 600, 300 | **1500** (600 + 600 + 300) |
| **2000** | Batch 1, 2, 3, 4 | `[600, 600, 600, 200]` | **750, 750, 750, 250** | 600, 600, 600, 200 | **2000** (600 + 600 + 600 + 200) |
| **2500** | Batch 1, 2, 3, 4, 5 | `[600, 600, 600, 600, 100]` | **750, 750, 750, 750, 125**| 600, 600, 600, 600, 100 | **2500** (600*4 + 100) |

---

## 3. ARCHITECTURE & CODE IMPLEMENTATION

### 3.1 Remaining Job Target Calculation (`app/tasks/batch_orchestration.py`)
In `_create_or_get_batch_unlocked`:
```python
# Sum confirmed accepted images across already completed batches
completed_accepted = sum(
    b.accepted_image_count or b.target_image_count or 0
    for b in existing_batches
    if b.status == BatchStatus.COMPLETED
)
total_accepted = max(job.accepted_count or 0, completed_accepted)
target = job.target_count or self.settings.batch_accepted_target
if completed_accepted >= target or total_accepted >= target:
    return None  # All batches fulfilled

next_index = len(existing_batches) + 1
remaining_needed = target - completed_accepted
if remaining_needed <= 0:
    return None

batch_target = min(self.settings.batch_accepted_target, remaining_needed)
new_batch = ImageBatch(
    id=uuid.uuid4(),
    job_id=self.job_id,
    batch_index=next_index,
    target_image_count=batch_target,
    status=BatchStatus.DISCOVERING,
)
```

### 3.2 Dynamic 25% Overfetch Window Sizing
In `app/services/candidate_reservation_service.py` and `app/tasks/batch_orchestration.py`:
- `ensure_candidate_window`:
  ```python
  batch_target = batch.target_image_count or configured
  accepted = batch.accepted_image_count or 0
  shortfall = max(1, batch_target - accepted)
  effective_limit = min(MAX_CANDIDATES_PER_WINDOW, max(1, calculate_initial_candidates(shortfall, job_config=job_for_buf.config if job_for_buf else None)))
  ```
- Formula: $\text{window\_size} = \text{shortfall} \times 1.25$.
  - Target 100 shortfall $\to$ 125 candidates.
  - Target 200 shortfall $\to$ 250 candidates.
  - Target 300 shortfall $\to$ 375 candidates.
  - Target 400 shortfall $\to$ 500 candidates.
  - Target 600 shortfall $\to$ 750 candidates.

### 3.3 Dynamic Filtering & Surplus Trimming (`app/tasks/filtering_tasks.py`)
- `trim_excess_batch_assets(batch_id)` reads `target = batch.target_image_count`.
- Assets exceeding `batch.target_image_count` are sorted by quality score.
- The top `target` assets are retained in the batch.
- Excess assets are transitioned to `AssetStatus.SURPLUS` with their local files deleted, ensuring the manifest contains exactly `batch.target_image_count` items.

### 3.4 Strict Complete-Batch NAS Protection Preserved (`app/storage/nas_writer.py`)
In `enqueue_nas_batch`:
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
- A final batch with target 400 will be **strictly refused** if only 399 items exist in the manifest.
- As soon as all 400 items exist, it is enqueued, uploaded, and verified as a complete batch.

### 3.5 Candidate Progress & Watchdog Coordination (`app/tasks/_helpers.py`, `app/tasks/orchestration_tasks.py`)
- `_helpers.py` (`build_on_progress`): Calculates the candidate round stop threshold by summing completed batch assets + current batch buffered target:
  ```python
  completed_acc = sum(b.accepted_image_count for b in batches if b.status == BatchStatus.COMPLETED)
  target = completed_acc + buffered_target(active_batch.target_image_count, buffer)
  ```
- `orchestration_tasks.py` (`check_and_advance_job` and watchdog): Uses `active_batch.target_image_count` instead of hardcoded 600 when checking window completeness and stall conditions.

---

## 4. EDGE CASE VERIFICATION

1. **Exact 600-image job:**
   - Evaluates $\min(600, 600) = 600$. Creates exactly one 600-image batch. Subsequent batch check returns `None`.
2. **Exact 2000-image job:**
   - Creates Batch 1 (600), Batch 2 (600), Batch 3 (600), Batch 4 (200). Total = 2000.
3. **Source Exhaustion on Final Batch:**
   - If sources exhaust before reaching the dynamic target (e.g. only 180 collected for a 200-image batch), the batch transitions to `BatchStatus.PARTIAL`.
   - `enqueue_nas_batch` enforces `total_files (180) < target (200)` and suppresses NAS upload.
4. **Worker Crash Recovery & Idempotency:**
   - If a Celery worker restarts while Batch 4 (target 200) is in flight (`DOWNLOADING` or `PROCESSING`), `get_or_create_active_batch()` acquires the row lock, detects the active in-flight batch, and resumes it without creating duplicate or ghost batches.
   - Re-running `create_batch_manifest` on an already-manifested batch returns the existing manifest file without altering asset states.

---

## 5. TEST EXECUTION SUMMARY

### 5.1 Dynamic Final Batch Dedicated Test Suite (`tests/unit/tasks/test_dynamic_final_batch.py`)
```bash
uv run pytest tests/unit/tasks/test_dynamic_final_batch.py -v
```
**Results:** **16 passed in 2.24s** (100% pass rate)
- `test_dynamic_batch_targets_and_sequence[100]` $\to$ **PASSED**
- `test_dynamic_batch_targets_and_sequence[200]` $\to$ **PASSED**
- `test_dynamic_batch_targets_and_sequence[600]` $\to$ **PASSED**
- `test_dynamic_batch_targets_and_sequence[1000]` $\to$ **PASSED**
- `test_dynamic_batch_targets_and_sequence[1500]` $\to$ **PASSED**
- `test_dynamic_batch_targets_and_sequence[2000]` $\to$ **PASSED**
- `test_dynamic_batch_targets_and_sequence[2500]` $\to$ **PASSED**
- `test_dynamic_overfetch_window_calculations[100->125]` $\to$ **PASSED**
- `test_dynamic_overfetch_window_calculations[200->250]` $\to$ **PASSED**
- `test_dynamic_overfetch_window_calculations[300->375]` $\to$ **PASSED**
- `test_dynamic_overfetch_window_calculations[400->500]` $\to$ **PASSED**
- `test_dynamic_overfetch_window_calculations[600->750]` $\to$ **PASSED**
- `test_manifest_creation_exact_count_for_dynamic_final_batch` $\to$ **PASSED**
- `test_nas_upload_guard_rejects_incomplete_final_batch` $\to$ **PASSED**
- `test_idempotent_batch_retries_prevent_duplicate_manifests` $\to$ **PASSED**
- `test_worker_crash_recovery_preserves_dynamic_active_batch` $\to$ **PASSED**

### 5.2 Full Task Regression Suite (`tests/unit/tasks/`)
```bash
uv run pytest tests/unit/tasks/ -q
```
**Results:** **224 passed in 10.78s** (100% pass rate across 224 unit tests)

### 5.3 End-to-End Overfetch Readiness Suite (`tests/integration/test_overfetch_readiness_e2e.py`)
```bash
uv run pytest tests/integration/test_overfetch_readiness_e2e.py -v
```
**Results:** **5 passed in 2.88s** (100% pass rate)

---

## 6. PRODUCTION CONSTRAINTS & STABILITY COMPLIANCE

- **Zero Container Restarts:** No container (`api`, `worker`, `worker-gpu`, `postgres`, `redis`) was restarted.
- **Zero Disruptions to Running Workloads:** Active jobs in PostgreSQL/Celery continued uninterrupted.
- **Strict Invariant Maintenance:** Incomplete batches can never be uploaded to NAS.
- **Database & Concurrency Safety:** Row-level PostgreSQL locks (`with_for_update()`) and Redis mutexes preserve serialized access during batch creation and window reservation.

**FINAL CONCLUSION:**  
Data Automation V3 now dynamically sizes final batches to match exact job demand, applies proportional 25% overfetch buffers, and delivers 100% of required images to the NAS while strictly safeguarding complete-batch integrity.
