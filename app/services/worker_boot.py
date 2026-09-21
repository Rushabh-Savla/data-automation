"""The time of the most recent worker boot, kept in Redis.

Read by watchdog_scan (via app/domain/recovery.py) to tell work orphaned by a
restart from work that is merely slow. The latest boot wins: if one container of
several restarts, only work last seen before *that* boot is suspect, and work on
the workers that kept running has been heartbeating since.
"""

from datetime import datetime, timezone

_BOOT_KEY = "watchdog:last_boot_at"
# Long past any plausible gap between restarts and the watchdog's next scan; it
# only exists so a marker from a long-decommissioned deployment does not linger.
_BOOT_TTL_S = 7 * 24 * 3600


def record_worker_boot(redis_client, now: datetime | None = None) -> None:
    now = now or datetime.now(timezone.utc)
    redis_client.set(_BOOT_KEY, str(now.timestamp()), ex=_BOOT_TTL_S)


def last_worker_boot(redis_client) -> datetime | None:
    raw = redis_client.get(_BOOT_KEY)
    if raw is None:
        return None
    try:
        return datetime.fromtimestamp(float(raw), timezone.utc)
    except (TypeError, ValueError):
        return None
