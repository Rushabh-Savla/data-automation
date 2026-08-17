"""Shared Redis client factory — used as Celery broker/result backend, for
pub/sub progress broadcasting, and for pause/cancel control flags."""

from functools import lru_cache

import redis

from app.core.config import get_settings


@lru_cache
def get_redis() -> redis.Redis:
    settings = get_settings()
    return redis.Redis.from_url(settings.redis_url, decode_responses=True)
