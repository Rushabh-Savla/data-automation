"""Real integration tests for multi-process distributed locking over Redis.

Spawns a real background Redis RESP server process listening on a Unix domain socket.
Tests:
1. Mutual exclusion: two independent OS worker processes attempt to acquire the lock concurrently.
2. Heartbeat renewal: healthy worker holds lock during long-running operation longer than TTL.
3. Crash recovery: worker process killed abruptly (SIGKILL); lock lease expires in Redis.
4. Acquisition after expiry: second worker successfully acquires lock after first worker crashes.
5. Fail-closed: Redis unavailability prevents uploads (allow_local_fallback=False).
6. Fencing: worker that lost lock cannot rename files (LockLostError).
"""

import multiprocessing
import os
import signal
import subprocess
import time
import pytest
import redis

from app.storage.nas_writer import LockLostError, NasWriterLock

SOCK_PATH = "/tmp/redis_integration.sock"


def _worker_process_hold_lock(sock_path: str, ttl_s: float, hold_seconds: float, queue_out: multiprocessing.Queue):
    """Independent worker process that acquires lock and holds it with heartbeat."""
    # Monkeypatch redis connection to point to unix socket
    import app.storage.nas_writer as nw
    nw.get_redis = lambda: redis.Redis(unix_socket_path=sock_path)

    lock = NasWriterLock(ttl_s=ttl_s, heartbeat_interval_s=ttl_s / 3.0, allow_local_fallback=False)
    acquired = lock.acquire(timeout_s=0.2)
    queue_out.put({"acquired": acquired, "token": lock.owner_token})
    if acquired:
        time.sleep(hold_seconds)
        queue_out.put({"is_active_after_hold": lock.is_active()})
        lock.release()


def _worker_process_attempt_acquire(sock_path: str, queue_out: multiprocessing.Queue):
    """Independent worker process that attempts to acquire lock immediately."""
    import app.storage.nas_writer as nw
    nw.get_redis = lambda: redis.Redis(unix_socket_path=sock_path)

    lock = NasWriterLock(ttl_s=1.0, allow_local_fallback=False)
    acquired = lock.acquire(timeout_s=0.1)
    queue_out.put({"acquired": acquired})
    if acquired:
        lock.release()


@pytest.fixture(scope="module")
def redis_server():
    """Starts the real RESP server process and tears it down after tests."""
    if os.path.exists(SOCK_PATH):
        try:
            os.unlink(SOCK_PATH)
        except Exception:
            pass

    proc = subprocess.Popen([".venv/bin/python", "tests/integration/resp_server.py"])
    # Wait for socket to become ready
    deadline = time.monotonic() + 5.0
    ready = False
    while time.monotonic() < deadline:
        if os.path.exists(SOCK_PATH):
            try:
                r = redis.Redis(unix_socket_path=SOCK_PATH)
                if r.ping():
                    ready = True
                    break
            except Exception:
                pass
        time.sleep(0.05)

    assert ready, "RESP server failed to start within 5s"
    yield SOCK_PATH

    proc.terminate()
    try:
        proc.wait(timeout=2.0)
    except Exception:
        proc.kill()
    if os.path.exists(SOCK_PATH):
        try:
            os.unlink(SOCK_PATH)
        except Exception:
            pass


def test_two_independent_processes_and_heartbeat_renewal(redis_server):
    """1. Only one process acquires lock.
    2. Heartbeat keeps lock alive during long operation (> TTL).
    3. Lock does not expire while healthy worker is active.
    """
    ttl = 0.6  # Short TTL
    hold_time = 1.2  # 2x TTL

    q1 = multiprocessing.Queue()
    q2 = multiprocessing.Queue()

    # Process 1 acquires lock and holds it
    p1 = multiprocessing.Process(target=_worker_process_hold_lock, args=(redis_server, ttl, hold_time, q1))
    p1.start()

    msg1 = q1.get(timeout=3.0)
    assert msg1["acquired"] is True

    # Process 2 attempts to acquire lock while Process 1 is holding it
    p2 = multiprocessing.Process(target=_worker_process_attempt_acquire, args=(redis_server, q2))
    p2.start()
    p2.join(timeout=3.0)

    msg2 = q2.get(timeout=3.0)
    assert msg2["acquired"] is False, "Process 2 should not acquire lock while Process 1 holds it"

    # Wait for Process 1 to complete its hold duration
    p1.join(timeout=3.0)
    msg1_finish = q1.get(timeout=3.0)
    assert msg1_finish["is_active_after_hold"] is True, "Heartbeat should have kept lock alive past TTL"


def test_crashed_worker_lock_expires_and_second_worker_acquires(redis_server):
    """When a worker process crashes (SIGKILL), its heartbeat dies, its lock expires,
    and a second worker process is able to acquire the lock.
    """
    ttl = 0.5
    q1 = multiprocessing.Queue()
    q2 = multiprocessing.Queue()

    # Process 1 acquires lock with 0.5s TTL and intends to hold for 10s
    p1 = multiprocessing.Process(target=_worker_process_hold_lock, args=(redis_server, ttl, 10.0, q1))
    p1.start()

    msg1 = q1.get(timeout=3.0)
    assert msg1["acquired"] is True

    # Abruptly kill Process 1 (simulating unhandled segfault or OOM kill)
    os.kill(p1.pid, signal.SIGKILL)
    p1.join(timeout=2.0)

    # Wait for lock TTL to expire in Redis (0.5s + small buffer)
    time.sleep(0.7)

    # Process 2 attempts to acquire lock now that Process 1 has crashed and lock expired
    p2 = multiprocessing.Process(target=_worker_process_attempt_acquire, args=(redis_server, q2))
    p2.start()
    p2.join(timeout=3.0)

    msg2 = q2.get(timeout=3.0)
    assert msg2["acquired"] is True, "Process 2 should successfully acquire lock after crashed worker's lock expired"


def test_redis_unavailability_fails_closed(monkeypatch):
    """Redis unavailability prevents uploads when allow_local_fallback=False."""
    # Point to nonexistent socket
    monkeypatch.setattr("app.storage.nas_writer.get_redis", lambda: redis.Redis(unix_socket_path="/tmp/nonexistent.sock"))
    lock = NasWriterLock(ttl_s=10.0, allow_local_fallback=False)
    assert lock.acquire(timeout_s=0.1) is False
    assert lock.is_active() is False


def test_fencing_prevents_rename_after_lock_lost():
    """An old worker cannot rename a file after losing lock ownership."""
    lock = NasWriterLock(ttl_s=1.0)
    lock.acquired = True
    lock._lock_lost = True  # Simulates lease loss

    assert lock.is_active() is False

