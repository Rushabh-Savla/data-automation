"""Cross-process test for staging backpressure hysteresis across independent OS processes.

Verifies:
1. Worker A crosses 80%: activates backpressure.
2. Worker B (in a separate OS process) observes PAUSED (True).
3. Usage falls to 75%: both Worker A and Worker B remain PAUSED (True).
4. Usage falls to 65% or below (64%): both Worker A and Worker B observe RESUMED (False).
5. Usage rises to 75%: both Worker A and Worker B remain RESUMED (False).
"""

import multiprocessing as mp
import time
from pathlib import Path
from unittest.mock import patch

from app.storage.ram_buffer import LocalStagingManager


def _worker_check(staging_dir_str: str, max_bytes: int, return_queue: mp.Queue):
    """Function executed in a completely independent OS process."""
    staging_path = Path(staging_dir_str)
    mgr = LocalStagingManager(staging_path=staging_path, max_bytes=max_bytes)
    result = mgr.is_staging_backpressure_active()
    return_queue.put(result)


def test_cross_process_backpressure_hysteresis(tmp_path):
    """Tests two independent OS processes verifying shared hysteresis without in-memory state leakage."""
    staging_dir = tmp_path / "cross_process_staging"
    staging_dir.mkdir(parents=True, exist_ok=True)
    max_bytes = 1000

    ctx = mp.get_context("spawn")
    data_file = staging_dir / "data.bin"

    mgr_a = LocalStagingManager(staging_path=staging_dir, max_bytes=max_bytes)

    # 1. Worker A: Disk usage crosses 80% (850 bytes = 85%)
    data_file.write_bytes(b"A" * 850)
    assert mgr_a.is_staging_backpressure_active() is True

    # 2. Worker B in separate OS process observes PAUSED
    q1 = ctx.Queue()
    p1 = ctx.Process(target=_worker_check, args=(str(staging_dir), max_bytes, q1))
    p1.start()
    p1.join(timeout=10)
    assert q1.get(timeout=5) is True  # Worker B observes PAUSED

    # 3. Usage falls to 75% (750 bytes): both Worker A and Worker B remain PAUSED
    data_file.write_bytes(b"A" * 750)
    assert mgr_a.is_staging_backpressure_active() is True

    q2 = ctx.Queue()
    p2 = ctx.Process(target=_worker_check, args=(str(staging_dir), max_bytes, q2))
    p2.start()
    p2.join(timeout=10)
    assert q2.get(timeout=5) is True  # Worker B remains PAUSED

    # 4. Usage falls to 64% (640 bytes <= 65% low watermark): both resume
    data_file.write_bytes(b"A" * 640)
    assert mgr_a.is_staging_backpressure_active() is False

    q3 = ctx.Queue()
    p3 = ctx.Process(target=_worker_check, args=(str(staging_dir), max_bytes, q3))
    p3.start()
    p3.join(timeout=10)
    assert q3.get(timeout=5) is False  # Worker B observes RESUMED

    # 5. Usage rises to 75% (750 bytes): both remain RESUMED
    data_file.write_bytes(b"A" * 750)
    assert mgr_a.is_staging_backpressure_active() is False

    q4 = ctx.Queue()
    p4 = ctx.Process(target=_worker_check, args=(str(staging_dir), max_bytes, q4))
    p4.start()
    p4.join(timeout=10)
    assert q4.get(timeout=5) is False  # Worker B remains RESUMED

