import redis

from app.core.redis_client import get_redis as _get_redis
from app.db.session import get_db

__all__ = ["get_db", "get_redis"]


def get_redis() -> redis.Redis:
    return _get_redis()
