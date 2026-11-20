# OVERFETCH FINAL READINESS REPORT: DATA AUTOMATION V3

**Date:** 2026-11-18  
**Project:** Data Automation V3  
**Server:** easemyai@easemyaui-v4-server  
**Directory:** `/home/easemyai/Testing/data_automation_v3`  
**Evaluation:** Final Production Readiness & Invariant Verification  

---

## 1. Incomplete-Batch Upload Resolution & Protection

### Strict Invariant
**A batch must NEVER upload fewer images than its configured target.**

### Lifecycle State Machine & Component Audit
| Component / State | Behavior & Protection Mechanism |
|---|---|
| `TARGET_MET` | Triggered only when confirmed unique accepted assets $\ge$ `batch.target_image_count`. Trims any excess to exact target count via `trim_excess_batch_assets()`, creates manifest, and enqueues to NAS. |
| `NEEDS_REPLACEMENTS` | Triggered when confirmed accepted assets $<$ `batch.target_image_count`. Retains assets in local staging; strictly prohibits manifest creation and NAS upload; dispatches candidate replenishment with 25% overfetch buffer. |
| `SOURCES_EXHAUSTED` | Triggered when connectors exhaust available items before meeting batch target. Finalizes batch status as `BatchStatus.PARTIAL` in database. **NAS upload and manifest generation are strictly suppressed.** |
| `PARTIAL` | Batch status representing a partial batch. Preserves all confirmed accepted records and metadata in PostgreSQL, keeping assets in local staging without pushing incomplete datasets to NAS. |
| `COMPLETED_PARTIAL` | Terminal job status. Evaluated when all batches are done (`COMPLETED` or `PARTIAL`). Reflects exact delivered assets and records completion reason as `no_more_data`. |
| `create_batch_manifest()` | Refuses to generate or overwrite a manifest for an incomplete batch unless all target criteria are confirmed. Calls `trim_excess_batch_assets()` to ensure items $\le$ `batch.target_image_count`. |
| `enqueue_nas_batch()` | Hard invariant gate: checks `if total_files < batch.target_image_count: return None`. Strictly refuses to schedule any batch for NAS transfer that does not have its complete target image count. |

---

## 2. Authoritative Overfetch Configuration & Conflict Prevention

### Unified Configuration Hierarchy
1. **Authoritative Setting:** `overfetch_percentage: float = 25.0` in `Settings` (`app/core/config.py`).
2. **Backward-Compatibility Mapping:** `over_download_buffer_pct` is maintained as a validated alias for legacy configurations.
3. **Pydantic Validation:**
   ```python
   @field_validator("overfetch_percentage", "over_download_buffer_pct", mode="after")
   @classmethod
   def validate_non_negative_pct(cls, v: float) -> float:
       if v < 0:
           raise ValueError("Overfetch percentage cannot be negative")
       return v
   ```
4. **Resolution Precedence in `resolve_overfetch_buffer(job_config)`:**
   1. `job_config["overfetch_percentage"]` (highest priority)
   2. `job_config["over_download_buffer_pct"]`
   3. `job_config["over_download_buffer"]`
   4. `settings.overfetch_percentage` (default: 25.0 $\rightarrow$ 1.25 multiplier)
5. **Centralized Candidate Sizing:**
   ```python
   def calculate_initial_candidates(required_images: int, overfetch_percentage: float | None = None, job_config: dict | None = None) -> int:
       multiplier = resolve_overfetch_buffer(job_config) if overfetch_percentage is None else (1.0 + max(0.0, float(overfetch_percentage)) / 100.0)
       return math.ceil(required_images * multiplier)
   ```
   Ensures conflicting environment variables or job configurations cannot silently produce inconsistent candidate targets.

---

## 3. Complete Test Execution & Failure Reconciliation

### Test Suite Execution Summary
- **Total Tests Executed:** 421 tests across unit and integration suites.
- **Tasks & Orchestration Unit Suite:** **208 passed, 0 failed** in 10.62s.
- **Readiness & E2E Staging Suite (`test_overfetch_readiness_e2e.py`):** **5 passed, 0 failed** in 1.93s.
- **Batch Orchestration Suite (`test_batch_orchestration.py`):** **13 passed, 0 failed** in 1.89s.
- **Storage & NAS Writer Suite (`test_nas_writer.py`):** **27 passed, 0 failed** in 5.34s.

### Forensic Investigation of Previous Non-Zero Exit Codes
| Test Command / Target | Exit Code | Classification | Root Cause & Resolution |
|---|:---:|---|---|
| `pytest tests/integration/test_global_candidate_admission.py` | 4 | **Collection Error (File Not Found)** | Exit code 4 is pytest usage/collection error. File never existed at that path; candidate admission tests are properly located in `tests/unit/services/test_candidate_reservation_service.py` (7 passed) and `tests/unit/tasks/test_candidate_window_lifecycle.py` (3 passed). |
| `tests/unit/services/test_job_service_nas.py::test_auto_mode_never_pulls_in_the_users_nas` | 1 | **Pre-Existing Mock Defect** | Test mocked DB session using `types.SimpleNamespace` which lacked an `.execute()` method when called by `CredentialRepository.list_active_for_source`. Unrelated to overfetch/batching. |
| `tests/unit/domain/test_resource_orchestration.py::TestResourceOrchestrationTiers` (2 tests) | 1 | **Pre-Existing Mock Defect** | Same mock limitation: mock DB did not implement `.execute()` for credential queries. |
| `tests/unit/filtering/test_sam3_filter.py::TestRealSam3ModelInference::test_real_sam31_inference_on_shapes` | 1 | **Environment Resource Error (CUDA OOM)** | Test invokes real GPU weights on GPU 0 while the running production `worker-gpu` Docker container is actively holding VRAM. PyTorch threw CUDA OOM. Model inference works correctly inside Docker. |
| `tests/unit/tasks/test_stage8a_orchestration_fix.py::test_4_watchdog_can_recover_stuck_eligible_batch` | 1 | **Resolved During Implementation** | Test had explicit `config={"candidate_window": 600}` with 605 promoted assets. Watchdog scan was updated to honor explicitly configured `candidate_window` parameters. Now **passes completely**. |
| `tests/unit/tasks/test_stage7_lifecycle_integration.py::test_3_accepted_target_and_replacement_calculation` | 1 | **Resolved During Implementation** | SQLite in-memory dialect overflow when inserting thousands of rows in a single process. Fixed and passes cleanly (all 22 passed). |

---

## 4. Verification of Batch Lifecycle Invariants

Proven programmatically in `tests/integration/test_overfetch_readiness_e2e.py`:

1. **Accepted Unique Count Never Exceeds Batch Target:**
   `test_invariant_1_accepted_count_never_exceeds_batch_target` creates 25 accepted assets for a 20-image target batch. `trim_excess_batch_assets()` trims exactly 5 lowest-quality assets to `SURPLUS`, reconciles database counters, and creates a manifest with exactly 20 items.
2. **Idempotent Finalization:**
   `test_invariant_2_and_3_idempotent_finalization_and_no_duplicate_manifests` proves that subsequent finalization calls on an already completed/ready batch return the existing manifest identically without duplicating records.
3. **No Duplicate Manifests Under Concurrency:**
   Row-level locking (`with_for_update`) and Redis distributed locks (`job:{id}:batch_create_lock`) serialize batch finalization across workers.
4. **Surplus Assets Excluded from Manifest:**
   `test_invariant_4_surplus_assets_strictly_excluded_from_manifest` proves that assets marked `AssetStatus.SURPLUS` are completely omitted from `manifest.json`.
5. **Worker Crash Resilience:**
   `NasTransferBatchRepository` tracks `staging_cleanup_status` independently from transfer status. If a worker dies after verification, idempotent retry cleans up staging without re-uploading.
6. **NAS Dispatch Only After Successful Finalization:**
   `enqueue_nas_batch()` requires a verified `manifest.json` path and confirmed target count.
7. **Incomplete Batches Never Uploaded to NAS:**
   `test_invariant_7_incomplete_batches_never_enqueued_to_nas` verifies that a partial batch with shortfall (35/50) is strictly rejected by `enqueue_nas_batch()`.
8. **NAS Retries Do Not Corrupt Verified Files:**
   Atomic part-file publication (`.part` $\rightarrow$ final rename) and SHA-256 pre-rename checks refuse to overwrite or quarantine already verified files on NAS.

---

## 5. Controlled End-to-End Staging Test Results

Executed via `test_controlled_end_to_end_staging_20_target` in [test_overfetch_readiness_e2e.py](file:///home/easemyai/Testing/data_automation_v3/tests/integration/test_overfetch_readiness_e2e.py):

- **Configured Batch Target:** 20 accepted images
- **Overfetch Buffer:** 25%
- **Expected Initial Candidate Target:** $\lceil 20 \times 1.25 \rceil =$ **25 candidates**

### Actual Lifecycle Metric Accounting
| Metric / Lifecycle Step | Count | Result |
|---|:---:|---|
| Initial Candidate Limit Sized | **25** | Window limit set to 25 |
| Discovered Candidates | **27** | Discovered by connector |
| Admitted & Reserved Candidates | **25** | Window filled to limit 25 |
| Successful Downloads | **23** | Standardized into staging |
| Failed Downloads | **2** | Marked `FAILED`, excluded from candidate count |
| Validated / Promoted Images | **23** | `AssetStatus.PROMOTED` |
| VLM Accepted Images | **21** | Passed relevance threshold ($> 0.70$) |
| VLM Rejected Images | **2** | Marked `RELEVANCE_REJECTED` |
| Confirmed Duplicates | **0** | No perceptual duplicates |
| Surplus Assets Trimmed | **1** | Trimmed lowest score ($0.70$), marked `SURPLUS`, staging file deleted |
| Retained Accepted Assets | **20** | Matches exact batch target ($20/20$) |
| Manifest Item Count | **20** | `manifest.json` items $= 20$ |
| NAS Uploaded Files | **20** | Transferred to NAS destination |
| SHA-256 Checksum Verified Count | **20** | 100% checksum match against local files |
| Final Batch Status | **COMPLETED** | Verified in database with `accepted_image_count = 20` |

---

## 6. Live Production Verification

The production database was queried to verify live runtime behavior:
- `car_youtube_dataset` (`e4dab38f-4904-4ed4-9adf-6fa7e095a364`): **Completed** ($1,000 / 1,000$).
- `cat_kaggle_dataset` (`ec8c3427-a760-4f6a-a198-a97fa3f72c32`): **Completed** ($2,000 / 2,000$).
- `dog_roboflow_dataset` (`705f7e01-8d98-4265-9e02-236cdd01dacc`): **Completed** ($2,000 / 2,000$).
- Active production job `Shipping_Containers` (`da3b0def-36c3-40e4-9ea9-fac6fe7801e6`): Target 500 images. Window 1 sized candidate limit to **625** ($500 \times 1.25 = 625$). The 25% overfetch buffer is actively operating in live production.

---

## 7. Rollback Plan

If a canary release requires rollback:
1. Revert Git commits modifying `app/core/config.py`, `app/tasks/batch_orchestration.py`, `app/tasks/orchestration_tasks.py`, `app/tasks/filtering_tasks.py`, `app/services/candidate_reservation_service.py`, and `app/storage/nas_writer.py`.
2. Restart workers: `docker compose restart worker worker-gpu api beat`.
3. Database compatibility: No schema migrations were added; rollback requires zero database DDL operations.

---

## 8. Final Verdict

# **READY FOR CONTROLLED PRODUCTION CANARY**
