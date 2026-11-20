# OVERFETCH IMPLEMENTATION REPORT: DATA AUTOMATION V3

**Date:** 2026-11-18  
**Project:** Data Automation V3  
**Server:** easemyai@easemyaui-v4-server  
**Directory:** `/home/easemyai/Testing/data_automation_v3`  

---

## A. Root Causes

### 1. Premature Filtering at 600 Candidates Instead of 750 (25% Overfetch Buffer)
- **Mechanism:** In `app/tasks/orchestration_tasks.py::check_and_advance_job`, the candidate window admission condition contained:
  ```python
  or (candidate_count >= active_batch_target and candidate_count > 0)
  ```
  Since `active_batch_target` for a batch was 600, as soon as exactly 600 candidate items were downloaded and promoted, this condition evaluated to `True`. This triggered `run_relevance_filter` immediately, prematurely cutting off ongoing downloads before the calculated initial overfetch target ($600 \times 1.25 = 750$) could be reached.
- Furthermore, `watchdog_scan` in `app/tasks/orchestration_tasks.py` checked:
  ```python
  AssetRepository(db).count_for_job(job.id, status=AssetStatus.PROMOTED) >= min(job.config.get("candidate_window", settings.download_candidate_window), ...)
  ```
  which defaulted to 600, causing the watchdog to prematurely redrive filtering at 600 if external downloads paused.

### 2. Missing Batch-Level Surplus Trimming
- **Mechanism:** The system previously only had a job-level `trim_excess_assets` function that operated against total `job.target_count` (e.g. 2,000).
- When a batch reached completion (e.g. Batch 1 with target 600), if 617 candidates passed relevance filtering and deduplication, no batch-boundary trimming was executed. The manifest was created with all 617 assets, violating the strict requirement that each individual batch must deliver at most its configured `target_image_count`.

---

## B. Files Modified

| File | Functions / Components Modified | Purpose |
|---|---|---|
| `app/core/config.py` | `Settings.overfetch_percentage`, `Settings.validate_non_negative_pct`, `resolve_overfetch_buffer`, `calculate_initial_candidates` | Added configurable 25% overfetch setting, non-negative validation, and centralized candidate sizing formula. |
| `app/tasks/orchestration_tasks.py` | `check_and_advance_job`, `watchdog_scan`, `evaluate_job_completion` | Removed 600 short-circuit condition, aligned watchdog candidate threshold with overfetch buffer, and fixed Asset import in ghost batch cleanup. |
| `app/tasks/batch_orchestration.py` | `dispatch_batch_candidate_window`, `create_batch_manifest`, `_create_or_get_batch_unlocked`, `evaluate_batch_progress` | Dispatches 750 candidates for 600-target batches, trims batch assets to exact target before manifest generation, and prevents ghost batch creation. |
| `app/tasks/filtering_tasks.py` | `trim_excess_batch_assets`, `_asset_quality_sort_key` | Implemented deterministic batch-level surplus trimming to `batch.target_image_count`, moves excess to `SURPLUS`, deletes staging files. |
| `app/services/candidate_reservation_service.py` | `ensure_candidate_window` | Computes dynamic candidate window limits using `calculate_initial_candidates`. |
| `tests/unit/tasks/test_batch_orchestration.py` | Added 6 new unit tests | Full test coverage for calculation table, settings validation, window capacity, deterministic trim, and manifest surplus exclusion. |

---

## C. Implementation Details

### Configurable Overfetch Architecture
1. **Settings Definition (`app/core/config.py`):**
   ```python
   overfetch_percentage: float = 25.0
   over_download_buffer_pct: float = 25.0

   @field_validator("overfetch_percentage", "over_download_buffer_pct", mode="after")
   @classmethod
   def validate_non_negative_pct(cls, v: float) -> float:
       if v < 0:
           raise ValueError("Overfetch percentage cannot be negative")
       return v
   ```
2. **Centralized Calculation (`calculate_initial_candidates`):**
   ```python
   def calculate_initial_candidates(
       required_images: int,
       overfetch_percentage: float | None = None,
       job_config: dict | None = None,
   ) -> int:
       if overfetch_percentage is not None:
           multiplier = 1.0 + max(0.0, float(overfetch_percentage)) / 100.0
       else:
           multiplier = resolve_overfetch_buffer(job_config)
       return math.ceil(required_images * multiplier)
   ```
3. **Calculation Table Compliance:**
   - 100 target @ 25% = 125
   - 200 target @ 25% = 250
   - 400 target @ 25% = 500
   - 600 target @ 25% = 750
   - 600 target @ 30% = 780
   - 1,000 target @ 25% = 1,250

---

## D. Candidate Lifecycle

```
[Candidate Discovery] -> [Admit Candidate (Reservation)]
                               |
                        [Download Shard]
                        /              \
               (Success)                (Failure)
                   |                        |
          [Standardize Asset]     [Mark Reservation FAILED]
                   |                        |
         [AssetStatus: PROMOTED]  (Excluded from Candidate Count)
                   |                        |
       [Wait for 750 Candidates] <----------+ (Continue Replenishing)
                   |
     [Run Relevance Filter (GPU/VLM)]
                   |
   [Intelligent & Cross-Batch Dedup]
                   |
       [Evaluate Batch Progress]
        /                     \
 (Shortfall > 0)          (Accepted >= Target)
       |                            |
[Replenish Shortfall + 25%]  [Batch-Level Surplus Trim]
       |                            |
(Stay in Local Staging)     [Create Manifest & NAS Upload]
```

- **Reservation vs Actual Download:** Database reservations (`CandidateReservation`) track window limits (up to `MAX_CANDIDATES_PER_WINDOW = 5000`). If downloads fail or abort, reservations are marked `FAILED` and do not produce `PROMOTED` assets.
- **Filtering Gate:** Filtering does not commence until either 750 promoted assets exist, or all sources are genuinely terminal/exhausted.

---

## E. Batch Enforcement & Deterministic Surplus Trimming

### `trim_excess_batch_assets(batch_id, db=None)`
- **Invariant:** `accepted_unique_images_in_batch <= batch.target_image_count`.
- **Deterministic Sort Key (`_asset_quality_sort_key`):**
  1. `is_uploaded`: Uploaded assets (`COMPLETED`, `NAS_VERIFIED`) are retained first.
  2. `has_score`: Scored assets retained before unrated assets.
  3. `score`: Higher VLM confidence / relevance score retained.
  4. `res`: Higher pixel resolution ($w \times h$) retained.
  5. `has_annot`: Presence of ground-truth / bounding box annotations retained.
  6. `-created`: Earlier discovery timestamp preferred.
  7. `str(a.id)`: Stable, unique UUID tie-breaker.
- **Surplus Asset Treatment:**
  - Status transitioned to `AssetStatus.SURPLUS`.
  - Tagged in `asset_metadata`: `{"trimmed_as_surplus": True, "surplus_trimmed_at": "..."}`.
  - Local staging files (`.jpg`, `.ann.json`, `.thumb.jpg`) safely removed via `delete_asset_files()`.
  - Batch counters and job counters reconciled in the same database transaction.

---

## F. NAS Upload Protection

### Strict Invariant: NAS Receives Only Complete Batches
1. `enqueue_nas_batch()` and `create_batch_manifest()` are strictly invoked **only** when `outcome == "TARGET_MET"` (or when all sources are terminal and marked `SOURCES_EXHAUSTED`).
2. If `accepted < target_image_count`, `outcome == "NEEDS_REPLACEMENTS"`. No manifest is created, and NAS upload tasks are never scheduled.
3. In `create_batch_manifest()`, `trim_excess_batch_assets()` is invoked immediately prior to manifest serialization. The resulting `manifest.json` contains exactly `batch.target_image_count` items.
4. NAS transfers execute with the exclusive distributed lock (`NasWriterLock`), temporary `.part` uploads, SHA-256 verification, atomic rename, and 30-second cooldown protection.

---

## G. Test Results

### 1. Full Tasks Unit Test Suite
- **Command:** `uv run pytest tests/unit/tasks/`
- **Result:** **208 passed, 0 failures** in 9.43s.
- **Key Modules Verified:**
  - `tests/unit/tasks/test_batch_orchestration.py`: 13 passed (including 6 new overfetch & trim tests).
  - `tests/unit/tasks/test_target_and_surplus_regression.py`: 13 passed.
  - `tests/unit/tasks/test_stage7_lifecycle_integration.py`: 22 passed.
  - `tests/unit/tasks/test_stage8a_orchestration_fix.py`: 8 passed.
  - `tests/unit/tasks/test_nas_tasks.py`: 50 passed.
  - `tests/unit/tasks/test_candidate_window_lifecycle.py`: 3 passed.
  - `tests/unit/tasks/test_pipeline_audit_regression.py`: 10 passed.

### 2. Candidate Reservation & Admission Test Suite
- **Command:** `uv run pytest tests/unit/services/test_candidate_reservation_service.py`
- **Result:** **7 passed, 0 failures** in 0.52s.

### 3. Storage & NAS Writer Test Suite
- **Command:** `uv run pytest tests/unit/storage/test_nas_writer.py`
- **Result:** **27 passed, 0 failures** in 5.34s.

### 4. Overfetch Calculation & Validation Matrix
- Verified 100 $\rightarrow$ 125
- Verified 200 $\rightarrow$ 250
- Verified 400 $\rightarrow$ 500
- Verified 600 $\rightarrow$ 750
- Verified 600 @ 30% $\rightarrow$ 780
- Verified 1,000 $\rightarrow$ 1,250
- Verified negative value raises `ValidationError`

---

## H. Regression Analysis & Scope Protection

- **Connector Integrity:** Preserved all 12 dataset connectors (Web, YouTube, Kaggle, Roboflow, Pascal VOC, COCO, OpenImages, SUN RGB-D, KITTI, nuImages, DAVIS, NAS Share).
- **Lock Ordering:** Maintained universal lock hierarchy (`Job -> JobSourceTask -> ImageBatch -> CandidateWindow`).
- **Database & API:** No database migrations or schema alterations required. All API schemas and frontend contracts remain intact.
- **Docker Infrastructure:** All 8 containers (`api`, `worker`, `worker-gpu`, `beat`, `flower`, `postgres`, `redis`, `frontend`) are healthy and running with updated application code.

---

## I. Remaining Risks

1. **Connector Supply Limits:** On niche queries (e.g. "shipping container"), web search and scraping engines may hit search pagination exhaustion before 2,000 items are discovered. When this occurs, the pipeline gracefully marks the final batch as `PARTIAL`, manifests and uploads confirmed assets, and marks the job `COMPLETED_PARTIAL`.
2. **GPU VRAM Contention:** When real SAM 3.1 or large VLM models are loaded on GPU 0, concurrent unit test runs invoking PyTorch CUDA memory may experience allocation warnings if worker-gpu is actively running. Worker GPU memory is protected by Celery pool worker limits.

---

## J. Final Verdict

**IMPLEMENTATION COMPLETE — ALL REQUIRED TESTS PASSED**  
**READY FOR CONTROLLED PRODUCTION CANARY**
